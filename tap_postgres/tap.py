"""Postgres tap class."""
from __future__ import annotations

import atexit
import io
import signal
from functools import cached_property
from typing import TYPE_CHECKING, Any, List

import paramiko
from singer_sdk import SQLTap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from sqlalchemy.engine.url import make_url
from sshtunnel import SSHTunnelForwarder

from tap_postgres.client import PostgresConnector, PostgresStream

if TYPE_CHECKING:
    from sqlalchemy.engine.url import URL


class TapPostgres(SQLTap):
    """Postgres tap class."""

    name = "tap-postgres"
    default_stream_class = PostgresStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            required=True,
            description=(
                "Example postgresql://postgres:postgres@localhost:5432/postgres"
            ),
        ),
       th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=True,
                    default=False,
                    description=(
                        "Enable an ssh tunnel (also known as bastion host), see the "
                        "other ssh_tunnel.* properties for more details"
                    ),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=True,
                    description="Hostname of the bastion host",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=True,
                    description="Username to connect to bastion host with",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Default SSH port",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=True,
                    secret=True,
                    description="Private Key for authentication to the bastion host",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=True,
                    secret=True,
                    description=(
                        "Private Key Password, leave None if no password is set"
                    ),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration",
        ),
    ).to_dict()

    @cached_property
    def connector(self) -> PostgresConnector:
        """Get a configured connector for this Tap.

        Connector is a singleton (one instance is used by the Tap and Streams).

        """
        # We mutate this url to use the ssh tunnel if enabled
        url = make_url(self.config["sqlalchemy_url"])
        ssh_config = self.config.get("ssh_tunnel", {})

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            url = self.ssh_tunnel_connect(ssh_config=ssh_config, url=url)

        return PostgresConnector(
            config=dict(self.config),
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def guess_key_type(self, key_data: str):
        """We are duplciating some logic from the ssh_tunnel package here,
        we could try to use their function instead."""
        for key_class in (
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))
            except paramiko.SSHException:
                continue
            else:
                return key
        raise ValueError("Could not determine the key type.")

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
            ssh_private_key=self.guess_key_type(ssh_config["ssh_tunnel.private_key"]),
            ssh_private_key_password=ssh_config.get("private_key_password"),
            remote_bind_address=(url.host, url.port),
        )
        self.ssh_tunnel.start()
        self.logger.info("SSH Tunnel started")
        # On program exit clean up, want to also catch signals
        atexit.register(self.clean_up)
        signal.signal(signal.SIGTERM, self.catch_signal)
        signal.signal(signal.SIGINT, self.catch_signal)

        # Swap the URL to use the tunnel
        return url.set(
            host=self.ssh_tunnel.local_bind_host,
            port=self.ssh_tunnel.local_bind_port,
        )

    def clean_up(self):
        self.logger.info("Shutting down SSH Tunnel")
        self.ssh_tunnel.stop()

    def catch_signal(self, signum, frame):
        exit(1)  # Be sure atexit is called, so clean_up gets called

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:  # type: ignore
            return self._catalog_dict  # type: ignore

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict = result
        return self._catalog_dict

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list.

        Returns
        -------
            List of discovered Stream objects.

        """
        return [
            PostgresStream(self, catalog_entry, connector=self.connector)
            for catalog_entry in self.catalog_dict["streams"]
        ]
