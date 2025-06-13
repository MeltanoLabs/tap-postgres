from __future__ import annotations

import sqlalchemy as sa
from singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema

from tap_postgres.client import PostgresConnector, PostgresStream
from tap_postgres.tap import TapPostgres


class DummyConnector(PostgresConnector):
    def get_table(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> sa.Table:
        return sa.Table("test_table", sa.MetaData(), sa.Column("id", sa.Integer))


def test_build_query():
    config = {
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "postgres",
        "stream_options": {
            "test_table": {
                "custom_where_clauses": ["id % 2 = 0", "id % 3 = 0"],
            },
        },
    }
    tap = TapPostgres(config=config, setup_mapper=False)
    catalog_entry = CatalogEntry(
        tap_stream_id="test_table",
        metadata=MetadataMapping.from_iterable(
            [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "inclusion": "available",
                        "selected": True,
                    },
                },
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {
                        "inclusion": "available",
                        "selected": True,
                    },
                },
            ]
        ),
        schema=Schema(
            properties={
                "id": Schema(type="integer"),
            },
            type="object",
        ),
        table="test_table",
    )
    stream = PostgresStream(tap, catalog_entry.to_dict(), connector=DummyConnector())
    assert (
        str(stream.build_query().compile()).replace("\n", "")
        == "SELECT test_table.id FROM test_table WHERE id % 2 = 0 AND id % 3 = 0"
    )
