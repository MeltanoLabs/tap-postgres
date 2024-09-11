import copy
import datetime
import decimal
import json

import pytest
import sqlalchemy as sa
from faker import Faker
from singer_sdk.testing import get_tap_test_class, suites
from singer_sdk.testing.runners import TapTestRunner
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIGINT,
    DATE,
    JSON,
    JSONB,
    TIME,
    TIMESTAMP,
)

from tap_postgres.tap import TapPostgres
from tests.settings import DB_SCHEMA_NAME, DB_SQLALCHEMY_URL
from tests.test_replication_key import TABLE_NAME, TapTestReplicationKey
from tests.test_selected_columns_only import (
    TABLE_NAME_SELECTED_COLUMNS_ONLY,
    TapTestSelectedColumnsOnly,
)

SAMPLE_CONFIG = {
    "start_date": datetime.datetime(2022, 11, 1).isoformat(),
    "sqlalchemy_url": DB_SQLALCHEMY_URL,
}

NO_SQLALCHEMY_CONFIG = {
    "start_date": datetime.datetime(2022, 11, 1).isoformat(),
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres",
}


def setup_test_table(table_name, sqlalchemy_url):
    """setup any state specific to the execution of the given module."""
    engine = sa.create_engine(sqlalchemy_url, future=True)
    fake = Faker()

    date1 = datetime.date(2022, 11, 1)
    date2 = datetime.date(2022, 11, 30)
    metadata_obj = sa.MetaData()
    test_replication_key_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("name", sa.String()),
    )
    with engine.begin() as conn:
        metadata_obj.create_all(conn)
        conn.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
        for _ in range(1000):
            insert = test_replication_key_table.insert().values(
                updated_at=fake.date_between(date1, date2), name=fake.name()
            )
            conn.execute(insert)


def teardown_test_table(table_name, sqlalchemy_url):
    engine = sa.create_engine(sqlalchemy_url, future=True)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE {table_name}"))


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


class TestTapPostgres_NOSQLALCHMY(TapPostgresTestNOSQLALCHEMY):  # noqa: N801
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
    table_name = "test_temporal_datatypes"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("column_date", DATE),
        sa.Column("column_time", TIME),
        sa.Column("column_timestamp", TIMESTAMP),
    )
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            column_date="2022-03-19",
            column_time="06:04:19.222",
            column_timestamp="1918-02-03 13:00:01",
        )
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
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
                schema_message["schema"]["properties"]["column_date"]["format"]
                == "date"
            )
            assert (
                schema_message["schema"]["properties"]["column_timestamp"]["format"]
                == "date-time"
            )
    assert test_runner.records[altered_table_name][0] == {
        "column_date": "2022-03-19",
        "column_time": "06:04:19.222000",
        "column_timestamp": "1918-02-03T13:00:01",
    }


def test_jsonb_json():
    """JSONB and JSON Objects weren't being selected, make sure they are now."""
    table_name = "test_jsonb_json"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("column_jsonb", JSONB),
        sa.Column("column_json", JSON),
    )

    rows = [
        {"column_jsonb": {"foo": "bar"}, "column_json": {"baz": "foo"}},
        {"column_jsonb": 3.14, "column_json": -9.3},
        {"column_jsonb": 22, "column_json": 10000000},
        {"column_jsonb": {}, "column_json": {}},
        {"column_jsonb": ["bar", "foo"], "column_json": ["foo", "baz"]},
        {"column_jsonb": True, "column_json": False},
    ]

    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(rows)
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
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
            assert schema_message["schema"]["properties"]["column_jsonb"] == {
                "type": [
                    "string",
                    "number",
                    "integer",
                    "array",
                    "object",
                    "boolean",
                    "null",
                ]
            }
            assert schema_message["schema"]["properties"]["column_json"] == {
                "type": [
                    "string",
                    "number",
                    "integer",
                    "array",
                    "object",
                    "boolean",
                    "null",
                ]
            }
    for i in range(len(rows)):
        assert test_runner.records[altered_table_name][i] == rows[i]


def test_jsonb_array():
    """ARRAYS of JSONB objects had incorrect schemas. See issue #331."""
    table_name = "test_jsonb_array"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("column_jsonb_array", ARRAY(JSONB)),
    )

    rows = [
        {"column_jsonb_array": [{"foo": "bar"}]},
        {"column_jsonb_array": [{"foo": 42}]},
        {"column_jsonb_array": [{"foo": 1.414}]},
        {"column_jsonb_array": [{"abc": "def"}, {"ghi": "jkl"}, {"mno": "pqr"}]},
    ]

    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(rows)
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
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
            assert schema_message["schema"]["properties"]["column_jsonb_array"] == {
                "items": {
                    "type": [
                        "string",
                        "number",
                        "integer",
                        "array",
                        "object",
                        "boolean",
                    ]
                },
                "type": ["array", "null"],
            }
    for i in range(len(rows)):
        assert test_runner.records[altered_table_name][i] == rows[i]


def test_numeric_types():
    """Schema was wrong for Decimal objects. Check they are correctly selected."""
    table_name = "test_decimal"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("my_numeric", sa.Numeric()),
        sa.Column("my_real", sa.REAL()),
    )
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            my_numeric=decimal.Decimal("3.14"),
            my_real=3.14,
        )
        conn.execute(insert)
        insert = table.insert().values(
            my_numeric=decimal.Decimal("12"),
            my_real=12,
        )
        conn.execute(insert)
        insert = table.insert().values(
            my_numeric=decimal.Decimal("10000.00001"),
            my_real=10000.00001,
        )
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"

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
            props = schema_message["schema"]["properties"]
            assert "number" in props["my_numeric"]["type"]
            assert "number" in props["my_real"]["type"]


def test_filter_schemas():
    """Only return tables from a given schema"""
    table_name = "test_filter_schemas"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id", BIGINT),
        schema="new_schema",
    )

    with engine.begin() as conn:
        conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS new_schema"))
        table.drop(conn, checkfirst=True)
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
        """Dislike this function and how TestRunner does this so just hacking it here.

        Want to be able to run exactly the catalog given
        """
        new_tap = self.new_tap()
        new_tap.sync_all()
        return True


def test_invalid_python_dates():  # noqa: PLR0912
    """Some dates are invalid in python, but valid in Postgres.

    Check out https://www.psycopg.org/psycopg3/docs/advanced/adapt.html#example-handling-infinity-date
    for more information.
    """
    table_name = "test_invalid_python_dates"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("date", DATE),
        sa.Column("datetime", sa.DateTime),
    )
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            date="4713-04-03 BC",
            datetime="4712-10-19 10:23:54 BC",
        )
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    # Alter config and then check the data comes through as a string
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
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
    with pytest.raises(ValueError):
        test_runner.sync_all()

    copied_config = copy.deepcopy(SAMPLE_CONFIG)
    # This should cause the same data to pass
    copied_config["dates_as_string"] = True
    tap = TapPostgres(config=copied_config)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
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
            assert schema_message["schema"]["properties"]["date"]["type"] == [
                "string",
                "null",
            ]
            assert schema_message["schema"]["properties"]["datetime"]["type"] == [
                "string",
                "null",
            ]
    assert test_runner.records[altered_table_name][0] == {
        "date": "4713-04-03 BC",
        "datetime": "4712-10-19 10:23:54 BC",
    }
