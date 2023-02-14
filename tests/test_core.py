import datetime

import pendulum
import pytest
import sqlalchemy
from faker import Faker
from singer_sdk.testing import get_tap_test_class, suites
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table
from test_replication_key import TABLE_NAME, TapTestReplicationKey

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

TapPostgresTest = get_tap_test_class(
    tap_class=TapPostgres,
    config=SAMPLE_CONFIG,
    catalog="tests/resources/data.json",
    custom_suites=[custom_test_replication_key],
)


class TestTapPostgres(TapPostgresTest):

    table_name = TABLE_NAME
    sqlalchemy_url = SAMPLE_CONFIG["sqlalchemy_url"]

    @pytest.fixture(scope="class")
    def resource(self):
        setup_test_table(self.table_name, self.sqlalchemy_url)
        yield
        teardown_test_table(self.table_name, self.sqlalchemy_url)
