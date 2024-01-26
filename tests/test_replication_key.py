"""Tests standard tap features using the built-in SDK tests library."""

import copy
import json

import pendulum
import sqlalchemy as sa
from singer_sdk.testing.runners import TapTestRunner
from singer_sdk.testing.templates import TapTestTemplate
from sqlalchemy.dialects.postgresql import TIMESTAMP

from tap_postgres.tap import TapPostgres
from tests.settings import DB_SCHEMA_NAME, DB_SQLALCHEMY_URL

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG = {
    "start_date": pendulum.datetime(2022, 11, 1).to_iso8601_string(),
    "sqlalchemy_url": DB_SQLALCHEMY_URL,
}


def replication_key_test(tap, table_name):
    """Originally built to address
    https://github.com/MeltanoLabs/tap-postgres/issues/9
    """
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
    #    json.dump(tap_catalog, f, indent=4)  # noqa: ERA001

    tap = TapPostgres(config=SAMPLE_CONFIG, catalog=tap_catalog)
    tap.sync_all()


def test_null_replication_key_with_start_date():
    """Null replication keys cause weird behavior. Check for appropriate handling.

    If a start date is provided, only non-null records with an replication key value
    greater than the start date should be synced.
    """
    table_name = "test_null_replication_key_with_start_date"
    engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("data", sa.String()),
        sa.Column("updated_at", TIMESTAMP),
    )
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            data="Alpha", updated_at=pendulum.datetime(2022, 10, 20).to_iso8601_string()
        )
        conn.execute(insert)
        insert = table.insert().values(
            data="Bravo", updated_at=pendulum.datetime(2022, 11, 20).to_iso8601_string()
        )
        conn.execute(insert)
        insert = table.insert().values(data="Zulu", updated_at=None)
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and altered_table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            stream["replication_key"] = "updated_at"
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] == []:
                    metadata["metadata"]["replication-method"] = "INCREMENTAL"
                    metadata["metadata"]["replication-key"] = "updated_at"

    test_runner = TapTestRunner(
        tap_class=TapPostgres,
        config=SAMPLE_CONFIG,
        catalog=tap_catalog,
    )
    test_runner.sync_all()
    assert len(test_runner.records[altered_table_name]) == 1  # Only record Bravo.


def test_null_replication_key_without_start_date():
    """Null replication keys cause weird behavior. Check for appropriate handling.

    If a start date is not provided, sync all records, including those with a null value
    for their replication key.
    """
    table_name = "test_null_replication_key_without_start_date"

    modified_config = copy.deepcopy(SAMPLE_CONFIG)
    modified_config["start_date"] = None
    engine = sa.create_engine(modified_config["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("data", sa.String()),
        sa.Column("updated_at", TIMESTAMP),
    )
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(
            data="Alpha", updated_at=pendulum.datetime(2022, 10, 20).to_iso8601_string()
        )
        conn.execute(insert)
        insert = table.insert().values(
            data="Bravo", updated_at=pendulum.datetime(2022, 11, 20).to_iso8601_string()
        )
        conn.execute(insert)
        insert = table.insert().values(data="Zulu", updated_at=None)
        conn.execute(insert)
    tap = TapPostgres(config=modified_config)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and altered_table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            stream["replication_key"] = "updated_at"
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] == []:
                    metadata["metadata"]["replication-method"] = "INCREMENTAL"
                    metadata["metadata"]["replication-key"] = "updated_at"

    test_runner = TapTestRunner(
        tap_class=TapPostgres,
        config=modified_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()
    assert (
        len(test_runner.records[altered_table_name]) == 3  # noqa: PLR2004
    )  # All three records.


class TapTestReplicationKey(TapTestTemplate):
    name = "replication_key"
    table_name = TABLE_NAME

    def test(self):
        replication_key_test(self.tap, self.table_name)
