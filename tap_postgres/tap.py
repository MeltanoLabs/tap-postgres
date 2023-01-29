"""Postgres tap class."""

from pathlib import Path, PurePath
from typing import List

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_postgres.client import PostgresConnector, PostgresStream


class TapPostgres(SQLTap):
    """Postgres tap class.

    This class uses a single connector to manage all connections for all of its
    streams.
    """

    name = "tap-postgres"
    default_stream_class = PostgresStream

    _connector: PostgresConnector = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            required=False,
            description="Full DB connection URL. E.g. 'postgresql://postgres:postgres@localhost:5432/postgres'",
        ),
        th.Property(
            "dialect+driver",
            th.StringType,
            required=False,
            default="postgresql+psycopg2",
            description="Dialect and driver for database. E.g. 'postgresql+pscyopg2' in 'postgresql+pscyopg2://user:pass@localhost:5432/postgres'",
        ),
        th.Property(
            "user",
            th.StringType,
            required=False,
            description="Username for database. E.g. 'user' in 'postgresql://user:pass@localhost:5432/postgres'",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="Password for database. (Set this via env var!) E.g. 'pass' in 'postgresql://user:pass@localhost:5432/postgres'",
        ),
        th.Property(
            "host",
            th.StringType,
            required=False,
            description="Hostname for database. E.g. 'localhost' in 'postgresql://user:pass@localhost:5432/postgres'",
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=False,
            default=5432,
            description="Port for database, as an integer. E.g. '5432' in 'postgresql://user:pass@localhost:5432/postgres'",
        ),
        th.Property(
            "database",
            th.StringType,
            required=False,
            description="Name of database to connect to. E.g. the last 'postgres' in 'postgresql://user:pass@localhost:5432/postgres'",
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_maps_config", th.ObjectType()),
    ).to_dict()

    def __init__(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        catalog: PurePath | str | dict | None = None,
        state: PurePath | str | dict | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        # Check that acceptable settings were passed to generate connection URL.
        # There's a few ways to do this in JSON Schema but it is schema draft dependent.
        # https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required
        assert (
            self.config.get("sqlalchemy_url") is not None
            and self.config.get("user") is None
            and self.config.get("password") is None
            and self.config.get("host") is None
        ) or (
            self.config.get("host") is not None
            and self.config.get("port") is not None
            and self.config.get("user") is not None
            and self.config.get("password") is not None
            and self.config.get("dialect+driver") is not None
            and self.config.get("sqlalchemy_url") is None
        ), (
            "Need either the sqlalchemy_url to be set or host, port, user,"
            + "password, and dialect+driver to be set."
        )

    def get_connector(self) -> PostgresConnector:
        """Get a configured connector for this Tap.

        Connector is a singleton (one instance is used by the Tap and Streams).
        """
        if not self._connector:
            self._connector = PostgresConnector(self.config)
        return self._connector

    def discover_streams(self) -> List[PostgresStream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: list[PostgresStream] = []
        return [
            PostgresStream(self, catalog_entry, connector=self.get_connector())
            for catalog_entry in self.catalog_dict["streams"]
        ]
