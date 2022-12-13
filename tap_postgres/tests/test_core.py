"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_postgres.tap import TapPostgres
from faker import Faker
import datetime
import pytest
from sqlalchemy import Table, Column, String, DateTime, Integer, MetaData

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapPostgres,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()

@pytest.fixture()
def sqlalchemy_connection(tap):
    #Used this function to avoid server side cursors which don't work with DDL
    return tap.default_stream_class.connector_class(dict(tap.config)).create_sqlalchemy_engine().connect()

@pytest.fixture()
def tap():
    return TapPostgres(config=SAMPLE_CONFIG)

@pytest.fixture()
def fake():
    return Faker()

def test_replication_key(sqlalchemy_connection, tap, fake):
    """Originally built to address https://github.com/MeltanoLabs/tap-postgres/issues/9"""
    date1 = datetime.date(2022,12,1)
    date2 = datetime.date(2022,12,30)
    metadata_obj = MetaData()
    table_name = "test_replication_key"

    test_replication_key_table = Table(
            table_name,
            metadata_obj,
            Column("id", Integer, primary_key=True),
            Column("updated_at", DateTime(), nullable=False),
            Column("name", String())
        )
    metadata_obj.create_all(sqlalchemy_connection)
    sqlalchemy_connection.execute(f"TRUNCATE TABLE {table_name}")

    for _ in range(10):
        #TODO this is slow we could speed this up by bulk inserting
        insert = test_replication_key_table.insert().values(updated_at=fake.date_between(date1,date2), name=fake.name())
        sqlalchemy_connection.execute(insert)

    #Create sink for table
    streams = tap.discover_streams()
    test_replication_key_stream = next(stream for stream in streams if stream.name.split("-")[1] == table_name)
    test_replication_key_stream.replication_key = "updated_at"
    test_replication_key_stream.sync()
