"""Postgres tap class."""

from typing import List

from singer_sdk import SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_postgres.client import PostgresConnector, PostgresStream


class TapPostgres(SQLTap):
    """Postgres tap class."""
    name = "tap-postgres"
    default_stream_class = PostgresStream
    _connector: PostgresConnector = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            required=True,
            description="Example postgresql://postgres:postgres@localhost:5432/postgres"
        ),
    ).to_dict()


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
