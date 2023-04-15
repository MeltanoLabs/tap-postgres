"""Postgres tap class."""
from __future__ import annotations

import atexit
import io
import signal
from functools import cached_property
from pathlib import PurePath
from typing import List

import paramiko
from singer_sdk import SQLTap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._secrets import SecretString, is_common_secret_key
from singer_sdk.helpers._util import read_json_file
from sqlalchemy.engine.url import make_url
from sshtunnel import SSHTunnelForwarder

from tap_postgres.client import PostgresConnector, PostgresStream


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
            "ssh_tunnel.enable",
            th.BooleanType,
            required=True,
            default=False,
            description=(
                "Enable an ssh tunnel (also known as bastian host), see the the other ssh.* properties for more details"
            ),
        ),
        th.Property(
            "ssh_tunnel.host",
            th.StringType,
            required=False,
            description=("Hostname of the bastian host"),
        ),
        th.Property(
            "ssh_tunnel.username",
            th.StringType,
            required=False,
            description=("Username to connect to bastian host with"),
        ),
        th.Property(
            "ssh_tunnel.port",
            th.IntegerType,
            required=False,
            default=22,
            description=("Default SSH port"),
        ),
        th.Property(
            "ssh_tunnel.private_key",
            th.StringType,
            required=False,
            description=("Private Key for authentcation to the bastian host"),
        ),
        th.Property(
            "ssh_tunnel.private_key_password",
            th.StringType,
            required=False,
            description=("Private Key Password, leave None if no password is set"),
        ),
    ).to_dict()

    @cached_property
    def connector(self) -> PostgresConnector:
        """Get a configured connector for this Tap.

        Connector is a singleton (one instance is used by the Tap and Streams).

        """
        # We mutate this url to use the ssh tunnel if enabled
        self.url = make_url(self.config["sqlalchemy_url"])

        if self.config["ssh_tunnel.enable"]:
            self.ssh_tunnel_connect()
        return PostgresConnector(
            config=dict(self.config),
            sqlalchemy_url=str(
                self.url
            ),  # Sometimes we mutuate the url to use the ssh tunnel
        )

    def guess_key_type(self, key_data):
        """We are duplciating some logic from the ssh_tunnel package here,
        we could try to use their function instead."""
        for key_class in [
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ]:
            try:
                key = key_class.from_private_key(io.StringIO(key_data))
                return key
            except paramiko.SSHException:
                continue
        raise ValueError("Could not determine the key type.")

    def ssh_tunnel_connect(self) -> None:
        self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
            ssh_address_or_host=(
                self.config["ssh_tunnel.host"],
                self.config["ssh_tunnel.port"],
            ),
            ssh_username=self.config["ssh_tunnel.username"],
            ssh_private_key=self.guess_key_type(self.config["ssh_tunnel.private_key"]),
            ssh_private_key_password=self.config.get("ssh_tunnel.private_key_password"),
            remote_bind_address=(self.url.host, self.url.port),
        )
        self.ssh_tunnel.start()
        self.logger.info("SSH Tunnel started")
        # On program exit clean up, want to also catch signals
        atexit.register(self.clean_up)
        signal.signal(signal.SIGTERM, self.catch_signal)
        signal.signal(signal.SIGINT, self.catch_signal)

        #  Swap URL and Port to use tunnel
        self.url = self.url.set(
            host=self.ssh_tunnel.local_bind_host, port=self.ssh_tunnel.local_bind_port
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
