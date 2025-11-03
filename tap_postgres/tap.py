"""Postgres tap class."""

from __future__ import annotations

import atexit
import copy
import io
import signal
import socket
import sys
import threading
from contextlib import suppress
from functools import cached_property
from os import chmod, path
from typing import TYPE_CHECKING, Any, cast

import paramiko
from singer_sdk import Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.singerlib import Catalog, Metadata, Schema
from singer_sdk.sql import SQLStream, SQLTap
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url

from tap_postgres.client import (
    PostgresConnector,
    PostgresLogBasedStream,
    PostgresStream,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Try to import MsgSpecWriter for better performance
try:
    from singer_sdk.contrib.msgspec import MsgSpecWriter

    _MSGSPEC_AVAILABLE = True
except ImportError:
    _MSGSPEC_AVAILABLE = False


REPLICATION_SLOT_PATTERN = "^(?!pg_)[A-Za-z0-9_]{1,63}$"


class SSHTunnelForwarder:
    """SSH Tunnel forwarder using paramiko.

    This class provides SSH tunnel functionality similar to sshtunnel package,
    but implemented directly with paramiko.
    """

    def __init__(
        self,
        ssh_address_or_host: tuple[str, int],
        ssh_username: str,
        ssh_pkey: paramiko.PKey,
        ssh_private_key_password: str | None,
        remote_bind_address: tuple[str, int],
    ) -> None:
        """Initialize SSH tunnel forwarder.

        Args:
            ssh_address_or_host: Tuple of (ssh_host, ssh_port)
            ssh_username: SSH username
            ssh_pkey: Paramiko private key object
            ssh_private_key_password: Private key password (optional)
            remote_bind_address: Tuple of (remote_host, remote_port)
        """
        self.ssh_host, self.ssh_port = ssh_address_or_host
        self.ssh_username = ssh_username
        self.ssh_pkey = ssh_pkey
        self.ssh_private_key_password = ssh_private_key_password
        self.remote_bind_host, self.remote_bind_port = remote_bind_address

        self.ssh_client: paramiko.SSHClient | None = None
        self.local_bind_host = "127.0.0.1"
        self.local_bind_port: int | None = None
        self._server_socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the SSH tunnel."""
        # Create SSH client
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to SSH server
        self.ssh_client.connect(
            hostname=self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username,
            pkey=self.ssh_pkey,
            passphrase=self.ssh_private_key_password,
        )

        # Create local socket for port forwarding
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.local_bind_host, 0))
        self._server_socket.listen(5)

        # Get the dynamically assigned local port
        self.local_bind_port = self._server_socket.getsockname()[1]

        # Start forwarding thread
        self._thread = threading.Thread(target=self._forward_tunnel, daemon=True)
        self._thread.start()

    def _forward_tunnel(self) -> None:
        """Forward connections through the SSH tunnel."""
        if self._server_socket is None or self.ssh_client is None:
            return

        while not self._stop_event.is_set():
            try:
                # Set timeout so we can check stop event periodically
                self._server_socket.settimeout(1.0)
                try:
                    local_socket, _ = self._server_socket.accept()
                except TimeoutError:
                    continue

                # Create channel through SSH tunnel
                transport = self.ssh_client.get_transport()
                if transport is None:
                    local_socket.close()
                    continue

                channel = transport.open_channel(
                    "direct-tcpip",
                    (self.remote_bind_host, self.remote_bind_port),
                    local_socket.getpeername(),
                )

                # Start forwarding data between local socket and channel
                threading.Thread(
                    target=self._forward_data,
                    args=(local_socket, channel),
                    daemon=True,
                ).start()
            except OSError:
                if not self._stop_event.is_set():
                    break

    def _forward_data(
        self,
        local_socket: socket.socket,
        channel: paramiko.Channel,
    ) -> None:
        """Forward data between local socket and SSH channel.

        Args:
            local_socket: Local socket
            channel: SSH channel
        """
        try:

            def forward_local_to_remote():
                while True:
                    data = local_socket.recv(4096)
                    if len(data) == 0:
                        break
                    channel.send(data)
                channel.close()

            def forward_remote_to_local():
                while True:
                    data = channel.recv(4096)
                    if len(data) == 0:
                        break
                    local_socket.send(data)
                local_socket.close()

            # Start both forwarding directions
            t1 = threading.Thread(target=forward_local_to_remote, daemon=True)
            t2 = threading.Thread(target=forward_remote_to_local, daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        except OSError:
            pass
        finally:
            with suppress(OSError):
                local_socket.close()
            with suppress(OSError):
                channel.close()

    def stop(self) -> None:
        """Stop the SSH tunnel."""
        self._stop_event.set()

        if self._server_socket:
            with suppress(OSError):
                self._server_socket.close()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        if self.ssh_client:
            self.ssh_client.close()


class TapPostgres(SQLTap):
    """Singer tap for Postgres."""

    name = "tap-postgres"
    package_name = "meltanolabs-tap-postgres"
    default_stream_class = PostgresStream

    # Use MsgSpecWriter if available for ~15-20% performance improvement
    if _MSGSPEC_AVAILABLE:
        message_writer_class = MsgSpecWriter

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        """Constructor.

        Should use JSON Schema instead
        See https://github.com/MeltanoLabs/tap-postgres/issues/141
        """
        super().__init__(*args, **kwargs)
        assert (self.config.get("sqlalchemy_url") is not None) or (
            self.config.get("host") is not None
            and self.config.get("port") is not None
            and self.config.get("user") is not None
            and self.config.get("password") is not None
        ), (
            "Need either the sqlalchemy_url to be set or host, port, user,"
            + " and password to be set"
        )

        # If log-based replication is used, sqlalchemy_url can't be used.
        assert (self.config.get("sqlalchemy_url") is None) or (
            self.config.get("replication_mode") != "LOG_BASED"
        ), "A sqlalchemy_url can't be used with log-based replication"

        # If sqlalchemy_url is not being used and ssl_enable is on, ssl_mode must have
        # one of six allowable values. If ssl_mode is verify-ca or verify-full, a
        # certificate authority must be provided to verify against.
        assert (
            (self.config.get("sqlalchemy_url") is not None)
            or (self.config.get("ssl_enable") is False)
            or (self.config.get("ssl_mode") in {"disable", "allow", "prefer", "require"})
            or (
                self.config.get("ssl_mode") in {"verify-ca", "verify-full"}
                and self.config.get("ssl_certificate_authority") is not None
            )
        ), (
            "ssl_enable is true but invalid values are provided for ssl_mode and/or"
            + "ssl_certificate_authority."
        )

        # If sqlalchemy_url is not being used and ssl_client_certificate_enable is on,
        # the client must provide a certificate and associated private key.
        assert (
            (self.config.get("sqlalchemy_url") is not None)
            or (self.config.get("ssl_client_certificate_enable") is False)
            or (
                self.config.get("ssl_client_certificate") is not None
                and self.config.get("ssl_client_private_key") is not None
            )
        ), (
            "ssl_client_certificate_enable is true but one or both of"
            + " ssl_client_certificate or ssl_client_private_key are unset."
        )

        self.ssh_tunnel: SSHTunnelForwarder | None = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "replication_slot_name",
            th.StringType(pattern=REPLICATION_SLOT_PATTERN),
            default="tappostgres",
            description=(
                "Name of the replication slot to use for logical replication. "
                "Must be unique for parallel extractions. "
                "Only applicable when replication_method is LOG_BASED."
                "- Contain only letters, numbers, and underscores. "
                "- Be less than or equal to 63 characters. "
                "- Not start with 'pg_'."
            ),
        ),
        th.Property(
            "host",
            th.StringType,
            description=(
                "Hostname for postgres instance. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=5432,
            description=(
                "The port on which postgres is awaiting connection. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "user",
            th.StringType,
            description=(
                "User name used to authenticate. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description=(
                "Password used to authenticate. Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "database",
            th.StringType,
            description=("Database name. " + "Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "max_record_count",
            th.IntegerType,
            default=None,
            description=("Optional. The maximum number of records to return in a single stream."),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description=("Example postgresql://[username]:[password]@localhost:5432/[db_name]"),
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            description=(
                "If an array of schema names is provided, the tap will only process "
                "the specified Postgres schemas and ignore others. If left blank, the "
                "tap automatically determines ALL available Postgres schemas."
            ),
        ),
        th.Property(
            "dates_as_string",
            th.BooleanType,
            description=(
                "Defaults to false, if true, date, and timestamp fields will be "
                "Strings. If you see ValueError: Year is out of range, "
                "try setting this to True."
            ),
            default=False,
        ),
        th.Property(
            "json_as_object",
            th.BooleanType,
            description=("Defaults to false, if true, json and jsonb fields will be Objects."),
            default=False,
        ),
        th.Property(
            "stream_options",
            th.ObjectType(
                additional_properties=th.ObjectType(
                    th.Property(
                        "custom_where_clauses",
                        th.ArrayType(th.StringType),
                        default=[],
                        description=(
                            "If an array of custom WHERE clauses is provided, the tap "
                            "will only process the records that match the WHERE "
                            "clauses. "
                            "The WHERE clauses are combined using the AND operator."
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=(
                        "Enable an ssh tunnel (also known as bastion server), see the "
                        "other ssh_tunnel.* properties for more details"
                    ),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=False,
                    description=(
                        "Host of the bastion server, this is the host we'll connect to via ssh"
                    ),
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=False,
                    description="Username to connect to bastion server",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Port to connect to bastion server",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Private Key for authentication to the bastion server",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=False,
                    secret=True,
                    default=None,
                    description=("Private Key Password, leave None if no password is set"),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration, this is a json object",
        ),
        th.Property(
            "ssl_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to use ssl to verify the server's identity. Use"
                + " ssl_certificate_authority and ssl_mode for further customization."
                + " To use a client certificate to authenticate yourself to the server,"
                + " use ssl_client_certificate_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_certificate_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to provide client-side certificates as a method of"
                + " authentication to the server. Use ssl_client_certificate and"
                + " ssl_client_private_key for further customization. To use SSL to"
                + " verify the server's identity, use ssl_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_mode",
            th.StringType,
            default="verify-full",
            description=(
                "SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION)"
                + " for more information. Must be one of disable, allow, prefer,"
                + " require, verify-ca, or verify-full."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_certificate_authority",
            th.StringType,
            default="~/.postgresql/root.crl",
            description=(
                "The certificate authority that should be used to verify the server's"
                + " identity. Can be provided either as the certificate itself (in"
                + " .env) or as a filepath to the certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_certificate",
            th.StringType,
            default="~/.postgresql/postgresql.crt",
            description=(
                "The certificate that should be used to verify your identity to the"
                + " server. Can be provided either as the certificate itself (in .env)"
                + " or as a filepath to the certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_private_key",
            th.StringType,
            default="~/.postgresql/postgresql.key",
            description=(
                "The private key for the certificate you provided. Can be provided"
                + " either as the certificate itself (in .env) or as a filepath to the"
                + " certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_storage_directory",
            th.StringType,
            default=".secrets",
            description=(
                "The folder in which to store SSL certificates provided as raw values."
                + " When a certificate/key is provided as a raw value instead of as a"
                + " filepath, it must be written to a file before it can be used. This"
                + " configuration option determines where that file is created."
            ),
        ),
        th.Property(
            "default_replication_method",
            th.StringType,
            default="FULL_TABLE",
            allowed_values=["FULL_TABLE", "INCREMENTAL", "LOG_BASED"],
            description=(
                "Replication method to use if there is not a catalog entry to override "
                "this choice. One of `FULL_TABLE`, `INCREMENTAL`, or `LOG_BASED`."
            ),
        ),
    ).to_dict()

    def get_sqlalchemy_url(self, config: Mapping[str, Any]) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast("str", config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername="postgresql+psycopg2",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
            query=self.get_sqlalchemy_query(config=config),
        )
        return cast("str", sqlalchemy_url)

    def get_sqlalchemy_query(self, config: Mapping[str, Any]) -> dict:
        """Get query values to be used for sqlalchemy URL creation.

        Args:
            config: The configuration for the connector.

        Returns:
            A dictionary with key-value pairs for the sqlalchemy query.
        """
        query = {}

        # ssl_enable is for verifying the server's identity to the client.
        if config["ssl_enable"]:
            ssl_mode = config["ssl_mode"]
            query.update({"sslmode": ssl_mode})
            if ssl_mode in ("verify-ca", "verify-full") and config.get("ssl_certificate_authority"):
                query["sslrootcert"] = self.filepath_or_certificate(
                    value=config["ssl_certificate_authority"],
                    alternative_name=config["ssl_storage_directory"] + "/root.crt",
                )

        # ssl_client_certificate_enable is for verifying the client's identity to the
        # server.
        if config["ssl_client_certificate_enable"]:
            query["sslcert"] = self.filepath_or_certificate(
                value=config["ssl_client_certificate"],
                alternative_name=config["ssl_storage_directory"] + "/cert.crt",
            )
            query["sslkey"] = self.filepath_or_certificate(
                value=config["ssl_client_private_key"],
                alternative_name=config["ssl_storage_directory"] + "/pkey.key",
                restrict_permissions=True,
            )
        return query

    def filepath_or_certificate(
        self,
        value: str,
        alternative_name: str,
        restrict_permissions: bool = False,
    ) -> str:
        """Provide the appropriate key-value pair based on a filepath or raw value.

        For SSL configuration options, support is provided for either raw values in
        .env file or filepaths to a file containing a certificate. This function
        attempts to parse a value as a filepath, and if no file is found, assumes the
        value is a certificate and creates a file named `alternative_name` to store the
        file.

        Args:
            value: Either a filepath or a raw value to be written to a file.
            alternative_name: The filename to use in case `value` is not a filepath.
            restrict_permissions: Whether to restrict permissions on a newly created
                file. On UNIX systems, private keys cannot have public access.

        Returns:
            A dictionary with key-value pairs for the sqlalchemy query

        """
        if path.isfile(value):
            return value

        with open(alternative_name, "wb") as alternative_file:
            alternative_file.write(value.encode("utf-8"))
        if restrict_permissions:
            chmod(alternative_name, 0o600)

        return alternative_name

    @cached_property
    def connector(self) -> PostgresConnector:
        """Get a configured connector for this Tap.

        Connector is a singleton (one instance is used by the Tap and Streams).

        """
        # We mutate this url to use the ssh tunnel if enabled
        url = make_url(self.get_sqlalchemy_url(config=self.config))
        ssh_config = self.config.get("ssh_tunnel", {})

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            url = self.ssh_tunnel_connect(ssh_config=ssh_config, url=url)

        return PostgresConnector(
            config=dict(self.config),
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def guess_key_type(self, key_data: str) -> paramiko.PKey:
        """Guess the type of the private key.

        Note: DSS keys are not supported as they were removed in paramiko 4.0
        due to being cryptographically weak.

        Args:
            key_data: The private key data to guess the type of.

        Returns:
            The private key object.

        Raises:
            ValueError: If the key type could not be determined.
        """
        for key_class in (
            paramiko.RSAKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))
            except paramiko.SSHException:  # noqa: PERF203
                continue
            else:
                return key

        errmsg = (
            "Could not determine the key type. Supported key types are RSA, ECDSA, "
            "and Ed25519. DSS keys are not supported as they were removed in "
            "paramiko 4.0 due to being cryptographically weak."
        )
        raise ValueError(errmsg)

    def ssh_tunnel_connect(self, *, ssh_config: dict[str, Any], url: URL) -> URL:
        """Connect to the SSH Tunnel and swap the URL to use the tunnel.

        Args:
            ssh_config: The SSH Tunnel configuration
            url: The original URL to connect to.

        Returns:
            The new URL to connect to, using the tunnel.
        """
        if url.host is None or url.port is None:
            msg = "Database host and port must be specified when using SSH tunnel"
            raise ValueError(msg)

        self.ssh_tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
            ssh_username=ssh_config["username"],
            ssh_pkey=self.guess_key_type(ssh_config["private_key"]),
            ssh_private_key_password=ssh_config.get("private_key_password"),
            remote_bind_address=(url.host, url.port),
        )
        self.ssh_tunnel.start()
        self.logger.info("SSH Tunnel started")
        # On program exit clean up, want to also catch signals
        atexit.register(self.clean_up)
        signal.signal(signal.SIGTERM, self.catch_signal)
        # Probably overkill to catch SIGINT, but needed for SIGTERM
        signal.signal(signal.SIGINT, self.catch_signal)

        # Swap the URL to use the tunnel
        return url.set(
            host=self.ssh_tunnel.local_bind_host,
            port=self.ssh_tunnel.local_bind_port,
        )

    def clean_up(self) -> None:
        """Stop the SSH Tunnel."""
        if self.ssh_tunnel:
            self.logger.info("Shutting down SSH Tunnel")
            self.ssh_tunnel.stop()

    def catch_signal(self, signum, frame) -> None:
        """Catch signals and exit cleanly.

        Args:
            signum: The signal number
            frame: The current stack frame
        """
        sys.exit(1)  # Calling this to be sure atexit is called, so clean_up gets called

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Override to prevent premature instantiation of the connector.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict: dict = result
        return self._catalog_dict

    @property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        new_catalog: Catalog = Catalog()
        modified_streams: list = []
        for stream in super().catalog.streams:
            stream_modified = False
            new_stream = copy.deepcopy(stream)
            if new_stream.replication_method == "LOG_BASED" and new_stream.schema.properties:
                for property in new_stream.schema.properties.values():
                    if "null" not in property.type:
                        if isinstance(property.type, list):
                            property.type.append("null")
                        else:
                            property.type = [property.type, "null"]
                if new_stream.schema.required:
                    stream_modified = True
                    new_stream.schema.required = None
                if "_sdc_deleted_at" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update(
                        {"_sdc_deleted_at": Schema(type=["string", "null"])}
                    )
                    new_stream.metadata.update(
                        {
                            ("properties", "_sdc_deleted_at"): Metadata(
                                Metadata.InclusionType.AVAILABLE, True, None
                            )
                        }
                    )
                if "_sdc_lsn" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update(
                        {"_sdc_lsn": Schema(type=["integer", "null"])}
                    )
                    new_stream.metadata.update(
                        {
                            ("properties", "_sdc_lsn"): Metadata(
                                Metadata.InclusionType.AVAILABLE, True, None
                            )
                        }
                    )
            if stream_modified:
                modified_streams.append(new_stream.tap_stream_id)
            new_catalog.add_stream(new_stream)
        if modified_streams:
            self.logger.info(
                "One or more LOG_BASED catalog entries were modified "
                f"({modified_streams=}) to allow nullability and include _sdc columns. "
                "See README for further information."
            )
        return new_catalog

    def discover_streams(self) -> Sequence[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        streams: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            if catalog_entry["replication_method"] == "LOG_BASED":
                streams.append(
                    PostgresLogBasedStream(self, catalog_entry, connector=self.connector)
                )
            else:
                streams.append(PostgresStream(self, catalog_entry, connector=self.connector))
        return streams
