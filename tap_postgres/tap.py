"""Postgres tap class."""
from __future__ import annotations

from functools import cached_property
from pathlib import PurePath
from typing import List

from singer_sdk import SQLTap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._secrets import SecretString, is_common_secret_key
from singer_sdk.helpers._util import read_json_file
from sqlalchemy.engine.url import make_url
from sshtunnel import SSHTunnelForwarder

from tap_postgres.client import PostgresConnector, PostgresStream


class TapPostgres(SQLTap):
    """Postgres tap class."""

    def _scary_load_config(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Stole from /singer_sdk/plugin_base.py __init__ method.

        Need to load _config before calling super().__init__ so that we can use the config to setup the ssh tunnel.

        Args:
            config: May be one or more paths, either as str or PurePath objects, or
                it can be a predetermined config dict.
            parse_env_config: True to parse settings from env vars.
            validate_config: True to require validation of config settings.

        Raises:
            ValueError: If config is not a dict or path string.
        """
        if not config:
            config_dict = {}
        elif isinstance(config, str) or isinstance(config, PurePath):
            config_dict = read_json_file(config)
        elif isinstance(config, list):
            config_dict = {}
            for config_path in config:
                # Read each config file sequentially. Settings from files later in the
                # list will override those of earlier ones.
                config_dict.update(read_json_file(config_path))
        elif isinstance(config, dict):
            config_dict = config
        else:
            raise ValueError(f"Error parsing config of type '{type(config).__name__}'.")
        if parse_env_config:
            self.logger.info("Parsing env var for settings config...")
            config_dict.update(self._env_var_config)
        else:
            self.logger.info("Skipping parse of env var settings...")
        for k, v in config_dict.items():
            if self._is_secret_config(k):
                config_dict[k] = SecretString(v)
        self._config = config_dict
        self._validate_config(raise_errors=validate_config)

    def __init__(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        catalog: PurePath | str | dict | None = None,
        state: PurePath | str | dict | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the SQL tap.

        See super() for details, this constructor is here for the ssh tunnel connection and teardown
        """
        self._scary_load_config(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        self.url = make_url(
            self.config["sqlalchemy_url"]
        )  # We mutate this url to use the ssh tunnel if enabled
        if self.config["ssh_tunnel.enable"]:
            self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
                ssh_address_or_host=(
                    self.config["ssh_tunnel.host"],
                    self.config["ssh_tunnel.port"],
                ),
                ssh_username=self.config["ssh_tunnel.username"],
                # TODO Update this to use the private_key from env instead of file
                ssh_private_key=self.config["ssh_tunnel.private_key"],
                ssh_private_key_password=self.config["ssh_tunnel.private_key_password"],
                remote_bind_address=(self.url.host, self.url.port),
            )
            new_host = self.ssh_tunnel.local_bind_address
            new_port = self.ssh_tunnel.local_bind_port

            self.url.host = new_host
            self.url.port = new_port

        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

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
            description=("Private Key for the bastian host"),
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
        return PostgresConnector(
            config=dict(self.config),
            sqlalchemy_url=str(
                self.url
            ),  # Sometimes we mutuate the url to use the ssh tunnel
        )

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
