import datetime
import json

import pendulum
import pytest
import sqlalchemy
from faker import Faker
from singer_sdk.testing import get_tap_test_class, suites
from singer_sdk.testing.runners import TapTestRunner
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table
from sqlalchemy.dialects.postgresql import JSONB
from test_replication_key import TABLE_NAME, TapTestReplicationKey
from test_selected_columns_only import (
    TABLE_NAME_SELECTED_COLUMNS_ONLY,
    TapTestSelectedColumnsOnly,
)

from tap_postgres.tap import TapPostgres

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


custom_test_replication_key = suites.TestSuite(
    kind="tap", tests=[TapTestReplicationKey]
)

custom_test_selected_columns_only = suites.TestSuite(
    kind="tap", tests=[TapTestSelectedColumnsOnly]
)

TapPostgresTest = get_tap_test_class(
    tap_class=TapPostgres,
    config=SAMPLE_CONFIG,
    catalog="tests/resources/data.json",
    custom_suites=[custom_test_replication_key],
)


# creating testing instance for isolated table in postgres
TapPostgresTestSelectedColumnsOnly = get_tap_test_class(
    tap_class=TapPostgres,
    config=SAMPLE_CONFIG,
    catalog="tests/resources/data_selected_columns_only.json",
    custom_suites=[custom_test_selected_columns_only],
)


class TestTapPostgres(TapPostgresTest):

    table_name = TABLE_NAME
    sqlalchemy_url = SAMPLE_CONFIG["sqlalchemy_url"]

    @pytest.fixture(scope="class")
    def resource(self):
        setup_test_table(self.table_name, self.sqlalchemy_url)
        yield
        teardown_test_table(self.table_name, self.sqlalchemy_url)


class TestTapPostgresSelectedColumnsOnly(TapPostgresTestSelectedColumnsOnly):

    table_name = TABLE_NAME_SELECTED_COLUMNS_ONLY
    sqlalchemy_url = SAMPLE_CONFIG["sqlalchemy_url"]

    @pytest.fixture(scope="class")
    def resource(self):
        setup_test_table(self.table_name, self.sqlalchemy_url)
        yield
        teardown_test_table(self.table_name, self.sqlalchemy_url)


def test_jsonb():
    """JSONB Objects weren't being selected, make sure they are now"""
    table_name = "test_jsonb"
    engine = sqlalchemy.create_engine(SAMPLE_CONFIG["sqlalchemy_url"])

    metadata_obj = MetaData()
    table = Table(
        table_name,
        metadata_obj,
        Column("column", JSONB),
    )
    with engine.connect() as conn:
        if table.exists(conn):
            table.drop(conn)
        metadata_obj.create_all(conn)
        insert = table.insert().values(column={"foo": "bar"})
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"public-{table_name}"
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and altered_table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] == []:
                    metadata["metadata"]["replication-method"] = "FULL_TABLE"

    test_runner = PostgresTestRunner(
        tap_class=TapPostgres, config=SAMPLE_CONFIG, catalog=tap_catalog
    )
    test_runner.sync_all()
    assert test_runner.records[altered_table_name][0] == {"column": {"foo": "bar"}}


class PostgresTestRunner(TapTestRunner):
    def run_sync_dry_run(self) -> bool:
        """
        Dislike this function and how TestRunner does this so just hacking it here.
        Want to be able to run exactly the catalog given
        """
        new_tap = self.new_tap()
        new_tap.sync_all()
        return True
