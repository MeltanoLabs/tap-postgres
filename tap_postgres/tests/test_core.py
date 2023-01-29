"""Tests standard tap features using the built-in SDK tests library."""
import datetime
import json
import os

import pendulum
import pytest
from faker import Faker
from singer_sdk.testing import get_standard_tap_tests
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table

from tap_postgres.tap import TapPostgres


@pytest.fixture
def sample_config_components(postgres_fixture):
    """A config that uses the user/password/host/port settings to configure
    database connection.
    """
    postgres_host_pytest_docker = postgres_fixture.get_addr("5432/tcp")[0]
    return {
        "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
        "host": os.getenv("TAP_POSTGRES_HOST", postgres_host_pytest_docker),
        "port": os.getenv("TAP_POSTGRES_PORT", 5432),
        "user": os.getenv("TAP_POSTGRES_USER", "postgres"),
        "password": os.getenv("TAP_POSTGRES_PASSWORD", "mypass"),
        "database": os.getenv("TAP_POSTGRES_DATABASE", "postgres"),
    }


@pytest.fixture
def sample_config_sqlalchemy_url(postgres_fixture):
    """A config that uses the sqlalchemy_url setting to configure
    database connection.
    """
    postgres_host_pytest_docker = postgres_fixture.get_addr("5432/tcp")[0]
    return {
        "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
        "sqlalchemy_url": os.getenv(
            "TAP_POSTGRES_SQLALCHEMY_URL",
            (
                "postgresql+psycopg2://postgres:mypass@"
                f"{postgres_host_pytest_docker}:5432/postgres"
            ),
        ),
    }


@pytest.fixture()
def sqlalchemy_connection(tap):
    # Used this function to avoid server side cursors which don't work with DDL
    return (
        tap.default_stream_class.connector_class(dict(tap.config))
        .create_sqlalchemy_engine()
        .connect()
    )


@pytest.fixture()
def tap(sample_config_components):
    sample_config = sample_config_components
    return TapPostgres(config=sample_config)


@pytest.fixture()
def fake():
    return Faker()


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(sample_config_components):
    """Run standard tap tests from the SDK."""
    sample_config = sample_config_components
    tests = get_standard_tap_tests(TapPostgres, config=sample_config)
    for test in tests:
        test()


class TestConfigChecks:
    """Test that database setting checks work and raise correctly."""

    def test_config_succeeds_on_sqlalchemy_url(
        self, sample_config_sqlalchemy_url, postgres_fixture
    ):
        sample_config = sample_config_sqlalchemy_url
        host = postgres_fixture.get_addr("5432/tcp")[0]
        t = TapPostgres(config=sample_config)

    def test_config_succeeds_on_components(self, sample_config_components):
        sample_config = sample_config_components
        t = TapPostgres(config=sample_config)

    def test_config_fails_on_components_missing(self, sample_config_components):
        sample_config = sample_config_components
        del sample_config["host"]
        with pytest.raises(KeyError):
            TapPostgres(config=sample_config)

    def test_config_fails_on_both_components_and_sqlalchemy_url_passed(
        self, sample_config_components
    ):
        sample_config = sample_config_components
        sample_config[
            "sqlalchemy_url"
        ] = "postgresql+psycopg2://postgres:mypass@127.0.0.1:5432/postgres"
        with pytest.raises(AssertionError):
            TapPostgres(config=sample_config)


def test_replication_key(
    sqlalchemy_connection, tap: TapPostgres, fake, sample_config_components
):
    """Originally built to address
    https://github.com/MeltanoLabs/tap-postgres/issues/9"""

    sample_config = sample_config_components
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
        config=sample_config
    )  # If first time running run_discovery won't see the new table
    tap.run_discovery()
    # TODO Switch this to using Catalog from _singerlib as it makes iterating over this stuff easier
    tap_catalog = json.loads(tap.catalog_json_text)
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            #  Without this the tap will not do an INCREMENTAL sync properly
            stream["replication_key"] = "updated_at"
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] == []:
                    metadata["metadata"]["replication-method"] = "INCREMENTAL"
                    metadata["metadata"]["replication-key"] = "updated_at"

    # Handy for debugging
    # with open('data.json', 'w', encoding='utf-8') as f:
    #    json.dump(tap_catalog, f, indent=4)

    tap = TapPostgres(config=sample_config, catalog=tap_catalog)
    tap.sync_all()
