"""Postgres tap class."""

from singer_sdk import SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from typing import List

from tap_postgres.client import PostgresStream


class TapPostgres(SQLTap):
    """Postgres tap class."""
    name = "tap-postgres"
    default_stream_class = PostgresStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="Hostname for database. E.g. 'localhost' in 'postgresql://user:pass@localhost:5432/postgres'"
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=True,
            description="Port for database. E.g. '5432' in 'postgresql://user:pass@localhost:5432/postgres'"
        ),
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="Username for database. E.g. 'user' in 'postgresql://user:pass@localhost:5432/postgres'"
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Password for database. (Set via env var!) E.g. 'pass' in 'postgresql://user:pass@localhost:5432/postgres'"
        ),
        th.Property(
            "dbname",
            th.StringType,
            required=True,
            description="Name of database to connect to. E.g. 'postgres' in 'postgresql://user:pass@localhost:5432/postgres'"
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_maps_config", th.ObjectType()),
    ).to_dict()
