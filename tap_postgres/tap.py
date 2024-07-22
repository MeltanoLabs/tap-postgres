"""Postgres tap class."""

from __future__ import annotations

import atexit
import copy
import io
import signal
import sys
from functools import cached_property
from os import chmod, path
from typing import TYPE_CHECKING, Any, Sequence, cast

import paramiko
from singer_sdk import SQLStream, SQLTap, Stream
from singer_sdk import typing as th
from singer_sdk._singerlib import (  # JSON schema typing helpers
    Catalog,
    Metadata,
    Schema,
)
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
from sshtunnel import SSHTunnelForwarder

from tap_postgres.client import (
    PostgresConnector,
    PostgresLogBasedStream,
    PostgresStream,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


class TapPostgres(SQLTap):
    """Singer tap for Postgres."""

    name = "tap-postgres"
    default_stream_class = PostgresStream

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
            or (
                self.config.get("ssl_mode") in {"disable", "allow", "prefer", "require"}
            )
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

    config_jsonschema = th.PropertiesList(
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
                "Password used to authenticate. "
                "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "database",
            th.StringType,
            description=(
                "Database name. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "max_record_count",
            th.IntegerType,
            default=None,
            description=(
                "Optional. The maximum number of records to return in a "
                "single stream."
            ),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description=(
                "Example postgresql://[username]:[password]@localhost:5432/[db_name]"
            ),
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
                        "Host of the bastion server, this is the host "
                        "we'll connect to via ssh"
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
                    description=(
                        "Private Key Password, leave None if no password is set"
                    ),
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
            return cast(str, config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername="postgresql+psycopg2",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
            query=self.get_sqlalchemy_query(config=config),
        )
        return cast(str, sqlalchemy_url)

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

        We are duplicating some logic from the ssh_tunnel package here,
        we could try to use their function instead.

        Args:
            key_data: The private key data to guess the type of.

        Returns:
            The private key object.

        Raises:
            ValueError: If the key type could not be determined.
        """
        for key_class in (
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))
            except paramiko.SSHException:  # noqa: PERF203
                continue
            else:
                return key

        errmsg = "Could not determine the key type."
        raise ValueError(errmsg)

    def ssh_tunnel_connect(self, *, ssh_config: dict[str, Any], url: URL) -> URL:
        """Connect to the SSH Tunnel and swap the URL to use the tunnel.

        Args:
            ssh_config: The SSH Tunnel configuration
            url: The original URL to connect to.

        Returns:
            The new URL to connect to, using the tunnel.
        """
        self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
            ssh_username=ssh_config["username"],
            ssh_private_key=self.guess_key_type(ssh_config["private_key"]),
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
            if (
                new_stream.replication_method == "LOG_BASED"
                and new_stream.schema.properties
            ):
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
                    PostgresLogBasedStream(
                        self, catalog_entry, connector=self.connector
                    )
                )
            else:
                streams.append(
                    PostgresStream(self, catalog_entry, connector=self.connector)
                )
        return streams
