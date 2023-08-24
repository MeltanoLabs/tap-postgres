import copy
import datetime
import decimal
import json

import pendulum
import pytest
import sqlalchemy
from faker import Faker
from singer_sdk.testing import get_tap_test_class, suites
from singer_sdk.testing.runners import TapTestRunner
from sqlalchemy import Column, DateTime, Integer, MetaData, Numeric, String, Table
from sqlalchemy.dialects.postgresql import BIGINT, DATE, JSON, JSONB, TIME, TIMESTAMP
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

NO_SQLALCHEMY_CONFIG = {
    "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres",
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

TapPostgresTestNOSQLALCHEMY = get_tap_test_class(
    tap_class=TapPostgres,
    config=NO_SQLALCHEMY_CONFIG,
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


class TestTapPostgres_NOSQLALCHMY(TapPostgresTestNOSQLALCHEMY):
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


def test_temporal_datatypes():
    """Dates were being incorrectly parsed as date times (issue #171).

    This test checks that dates are being parsed correctly, and additionally implements
    schema checks, and performs similar tests on times and timestamps.
    """
    table_name = "test_date"
    engine = sqlalchemy.create_engine(SAMPLE_CONFIG["sqlalchemy_url"])

    metadata_obj = MetaData()
    table = Table(
        table_name,
        metadata_obj,
        Column("column_date", DATE),
        Column("column_time", TIME),
        Column("column_timestamp", TIMESTAMP),
    )
    with engine.connect() as conn:
        if table.exists(conn):
            table.drop(conn)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            column_date="2022-03-19",
            column_time="06:04:19.222",
            column_timestamp="1918-02-03 13:00:01",
        )
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
    for schema_message in test_runner.schema_messages:
        if (
            "stream" in schema_message
            and schema_message["stream"] == altered_table_name
        ):
            assert (
                "date"
                == schema_message["schema"]["properties"]["column_date"]["format"]
            )
            assert (
                "date-time"
                == schema_message["schema"]["properties"]["column_timestamp"]["format"]
            )
    assert test_runner.records[altered_table_name][0] == {
        "column_date": "2022-03-19",
        "column_time": "06:04:19.222000",
        "column_timestamp": "1918-02-03T13:00:01",
    }


def test_jsonb_json():
    """JSONB and JSON Objects weren't being selected, make sure they are now"""
    table_name = "test_jsonb_json"
    engine = sqlalchemy.create_engine(SAMPLE_CONFIG["sqlalchemy_url"])

    metadata_obj = MetaData()
    table = Table(
        table_name,
        metadata_obj,
        Column("column_jsonb", JSONB),
        Column("column_json", JSON),
    )
    with engine.connect() as conn:
        if table.exists(conn):
            table.drop(conn)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            column_jsonb={"foo": "bar"},
            column_json={"baz": "foo"},
        )
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
    for schema_message in test_runner.schema_messages:
        if (
            "stream" in schema_message
            and schema_message["stream"] == altered_table_name
        ):
            assert "object" in schema_message["schema"]["properties"]["column_jsonb"]["type"]
            assert "object" in schema_message["schema"]["properties"]["column_json"]["type"]
    assert test_runner.records[altered_table_name][0] == {
        "column_jsonb": {"foo": "bar"},
        "column_json": {"baz": "foo"}
    }


def test_decimal():
    """Schema was wrong for Decimal objects. Check they are correctly selected."""
    table_name = "test_decimal"
    engine = sqlalchemy.create_engine(SAMPLE_CONFIG["sqlalchemy_url"])

    metadata_obj = MetaData()
    table = Table(
        table_name,
        metadata_obj,
        Column("column", Numeric()),
    )
    with engine.connect() as conn:
        if table.exists(conn):
            table.drop(conn)
        metadata_obj.create_all(conn)
        insert = table.insert().values(column=decimal.Decimal("3.14"))
        conn.execute(insert)
        insert = table.insert().values(column=decimal.Decimal("12"))
        conn.execute(insert)
        insert = table.insert().values(column=decimal.Decimal("10000.00001"))
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
    for schema_message in test_runner.schema_messages:
        if (
            "stream" in schema_message
            and schema_message["stream"] == altered_table_name
        ):
            assert "number" in schema_message["schema"]["properties"]["column"]["type"]


def test_filter_schemas():
    """Only return tables from a given schema"""
    table_name = "test_filter_schemas"
    engine = sqlalchemy.create_engine(SAMPLE_CONFIG["sqlalchemy_url"])

    metadata_obj = MetaData()
    table = Table(table_name, metadata_obj, Column("id", BIGINT), schema="new_schema")

    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS new_schema")
        if table.exists(conn):
            table.drop(conn)
        metadata_obj.create_all(conn)
    filter_schemas_config = copy.deepcopy(SAMPLE_CONFIG)
    filter_schemas_config.update({"filter_schemas": ["new_schema"]})
    tap = TapPostgres(config=filter_schemas_config)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"new_schema-{table_name}"
    # Check that the only stream in the catalog is the one table put into new_schema
    assert len(tap_catalog["streams"]) == 1
    assert tap_catalog["streams"][0]["stream"] == altered_table_name


class PostgresTestRunner(TapTestRunner):
    def run_sync_dry_run(self) -> bool:
        """
        Dislike this function and how TestRunner does this so just hacking it here.
        Want to be able to run exactly the catalog given
        """
        new_tap = self.new_tap()
        new_tap.sync_all()
        return True
