import pendulum
import pytest
import sqlalchemy
from singer_sdk.testing import get_tap_test_class, suites
from singer_sdk.testing.templates import TapTestTemplate
from test_replication_key import (
    TapTestReplicationKey,
    setup_test_table,
    teardown_test_table,
    TABLE_NAME
)

from tap_postgres.tap import TapPostgres

SAMPLE_CONFIG = {
    "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
    "sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres",
}

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
