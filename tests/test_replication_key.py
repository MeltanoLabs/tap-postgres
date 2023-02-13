"""Tests standard tap features using the built-in SDK tests library."""
import datetime
import json

import pendulum
import sqlalchemy
from faker import Faker
from singer_sdk.testing.templates import TapTestTemplate
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table

from tap_postgres.tap import TapPostgres

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG = {
    "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
    "sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres",
}


def setup_test_table(table_name, sqlalchemy_url):
    """setup any state specific to the execution of the given module."""
    engine = sqlalchemy.create_engine(sqlalchemy_url)
    fake = Faker()

    date1 = datetime.date(2022, 11, 1)
    date2 = datetime.date(2022, 11, 30)
    metadata_obj = MetaData()
    test_replication_key_table = Table(
        table_name,
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("updated_at", DateTime(), nullable=False),
        Column("name", String()),
    )
    with engine.connect() as conn:
        metadata_obj.create_all(conn)
        conn.execute(f"TRUNCATE TABLE {table_name}")
        for _ in range(1000):
            insert = test_replication_key_table.insert().values(
                updated_at=fake.date_between(date1, date2), name=fake.name()
            )
            conn.execute(insert)


def teardown_test_table(table_name, sqlalchemy_url):
    engine = sqlalchemy.create_engine(sqlalchemy_url)
    with engine.connect() as conn:
        conn.execute(f"DROP TABLE {table_name}")


def replication_key_test(tap, table_name):
    """Originally built to address
    https://github.com/MeltanoLabs/tap-postgres/issues/9
    """
    # Create sink for table
    # tap = TapPostgres(
    #     config=SAMPLE_CONFIG
    # )  # If first time running run_discovery won't see the new table
    tap.run_discovery()
    # TODO Switch this to using Catalog from _singerlib as it makes iterating
    # over this stuff easier
    tap_catalog = json.loads(tap.catalog_json_text)
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            # Without this the tap will not do an INCREMENTAL sync properly
            stream["replication_key"] = "updated_at"
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


class TapTestReplicationKey(TapTestTemplate):
    name = "replication_key"
    table_name = TABLE_NAME

    def test(self):
        replication_key_test(self.tap, self.table_name)
