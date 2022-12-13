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

def test_replication_key():
    #TODO pull out stuff here
    fake = Faker()
    date1 = datetime.date(2022,12,1)
    date2 = datetime.date(2022,12,30)
    fake.date_between(date1, date2)
    pg_tap = TapPostgres(config=SAMPLE_CONFIG)
    pg_connector = pg_tap.default_stream_class.connector_class(dict(pg_tap.config))
    sqlalchemy_connection = pg_connector.create_sqlalchemy_engine().connect()
    metadata_obj = MetaData()

    test_replication_key_table = Table(
            "test_replication_key",
            metadata_obj,
            Column("id", Integer, primary_key=True),
            Column("updated_at", DateTime(), nullable=False),
            Column("name", String())
        )
    metadata_obj.create_all(sqlalchemy_connection)
    sqlalchemy_connection.execute(f"TRUNCATE TABLE {test_replication_key_table.name}")

    for _ in range(1000):
        insert = test_replication_key_table.insert().values(updated_at=fake.date_between(date1,date2), name=fake.name())
        sqlalchemy_connection.execute(insert)

    #Create sink for table
    streams = pg_tap.discover_streams()
    test_replication_key_stream = next(stream for stream in streams if stream.name.split("-")[1] == "test_replication_key")
    test_replication_key_stream.replication_key = "updated_at"
    test_replication_key_stream.sync()
