"""Tests standard tap features using the built-in SDK tests library."""
import datetime
import json

import pendulum
import pytest
from faker import Faker
from singer_sdk.testing import get_standard_tap_tests
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table

from tap_postgres.tap import TapPostgres

SAMPLE_CONFIG = {
    "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
    "sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapPostgres, config=SAMPLE_CONFIG)
    for test in tests:
        test()


@pytest.fixture()
def sqlalchemy_connection(tap):
    # Used this function to avoid server side cursors which don't work with DDL
    return (
        tap.default_stream_class.connector_class(dict(tap.config))
        .create_sqlalchemy_engine()
        .connect()
    )


@pytest.fixture()
def tap():
    return TapPostgres(config=SAMPLE_CONFIG)


@pytest.fixture()
def fake():
    return Faker()


def test_replication_key(sqlalchemy_connection, tap: TapPostgres, fake):
    """Originally built to address https://github.com/MeltanoLabs/tap-postgres/issues/9"""
    date1 = datetime.date(2022, 11, 1)
    date2 = datetime.date(2022, 11, 30)
    metadata_obj = MetaData()
    table_name = "test_replication_key"

    test_replication_key_table = Table(
        table_name,
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("updated_at", DateTime(), nullable=False),
        Column("name", String()),
    )
    metadata_obj.create_all(sqlalchemy_connection)
    sqlalchemy_connection.execute(f"TRUNCATE TABLE {table_name}")

    for _ in range(1000):
        insert = test_replication_key_table.insert().values(
            updated_at=fake.date_between(date1, date2), name=fake.name()
        )
        sqlalchemy_connection.execute(insert)

    # Create sink for table
    tap = TapPostgres(
        config=SAMPLE_CONFIG
    )  # If first time running run_discovery won't see the new table
    tap.run_discovery()
    # TODO Switch this to using Catalog from _singerlib as it makes iterating over this stuff easier
    tap_catalog = json.loads(tap.catalog_json_text)
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            stream[
                "replication_key"
            ] = "updated_at"  # Without this the tap will not do an INCREMENTAL sync properly
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] == []:
                    metadata["metadata"]["replication-method"] = "INCREMENTAL"
                    metadata["metadata"]["replication-key"] = "updated_at"

    # Handy for debugging
    # with open('data.json', 'w', encoding='utf-8') as f:
    #    json.dump(tap_catalog, f, indent=4)

    tap = TapPostgres(config=SAMPLE_CONFIG, catalog=tap_catalog)
    tap.sync_all()
