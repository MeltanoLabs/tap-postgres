"""Postgres tap class."""

from typing import List

from singer_sdk import SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_postgres.client import PostgresStream


class TapPostgres(SQLTap):
    """Postgres tap class."""
    name = "tap-postgres"
    default_stream_class = PostgresStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            required=True,
            description="Example postgresql://postgres:postgres@localhost:5432/postgres"
        ),
    ).to_dict()
