"""Tests standard tap features using the built-in SDK tests library."""

import copy
import datetime
import zoneinfo

import sqlalchemy as sa
from singer_sdk.singerlib import Catalog, StreamMetadata
from singer_sdk.testing.runners import TapTestRunner
from singer_sdk.testing.templates import TapTestTemplate
from sqlalchemy.dialects.postgresql import TIMESTAMP

from tap_postgres.tap import TapPostgres
from tests.settings import DB_SCHEMA_NAME, DB_SQLALCHEMY_URL

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG = {
    "start_date": datetime.datetime(2022, 11, 1).isoformat(),
    "sqlalchemy_url": DB_SQLALCHEMY_URL,
}


def replication_key_test(tap: TapPostgres, table_name):
    """Originally built to address
    https://github.com/MeltanoLabs/tap-postgres/issues/9
    """
    tap.run_discovery()
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    for stream in tap_catalog.streams:
        if stream.stream and table_name not in stream.stream:
            for metadata in stream.metadata.values():
                metadata.selected = False
        else:
            # Without this the tap will not do an INCREMENTAL sync properly
            stream.replication_key = "updated_at"
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "updated_at"

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
            data="Alpha", updated_at=datetime.datetime(2022, 10, 20).isoformat()
        )
        conn.execute(insert)
        insert = table.insert().values(
            data="Bravo", updated_at=datetime.datetime(2022, 11, 20).isoformat()
        )
        conn.execute(insert)
        insert = table.insert().values(data="Zulu", updated_at=None)
        conn.execute(insert)
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    for stream in tap_catalog.streams:
        if stream.stream and altered_table_name not in stream.stream:
            for metadata in stream.metadata.values():
                metadata.selected = False
        else:
            stream.replication_key = "updated_at"
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "updated_at"

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
            data="Alpha", updated_at=datetime.datetime(2022, 10, 20).isoformat()
        )
        conn.execute(insert)
        insert = table.insert().values(
            data="Bravo", updated_at=datetime.datetime(2022, 11, 20).isoformat()
        )
        conn.execute(insert)
        insert = table.insert().values(data="Zulu", updated_at=None)
        conn.execute(insert)
    tap = TapPostgres(config=modified_config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    for stream in tap_catalog.streams:
        if stream.stream and altered_table_name not in stream.stream:
            for metadata in stream.metadata.values():
                metadata.selected = False
        else:
            stream.replication_key = "updated_at"
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "updated_at"

    test_runner = TapTestRunner(
        tap_class=TapPostgres,
        config=modified_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()
    assert (
        len(test_runner.records[altered_table_name]) == 3  # noqa: PLR2004
    )  # All three records.


def test_replication_key_buffer_seconds():
    """Test that replication_key_buffer_seconds excludes recently updated records."""
    table_name = "test_replication_key_buffer_seconds"
    
    # Create a config with a 300 second (5 minute) buffer
    modified_config = copy.deepcopy(SAMPLE_CONFIG)
    modified_config["replication_key_buffer_seconds"] = 300
    
    engine = sa.create_engine(modified_config["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("data", sa.String()),
        sa.Column("updated_at", TIMESTAMP),
    )
    
    current_time = datetime.datetime.now(datetime.timezone.utc)
    
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        
        # Insert record that's older than buffer (should be included)
        old_record_time = current_time - datetime.timedelta(seconds=60*6)  # 6 minutes
        insert = table.insert().values(
            data="OldRecord", 
            updated_at=old_record_time
        )
        conn.execute(insert)
        
        # Insert record that's within buffer (should be excluded)
        recent_record_time = current_time - datetime.timedelta(seconds=60*4)  # 4 minutes ago
        insert = table.insert().values(
            data="RecentRecord", 
            updated_at=recent_record_time
        )
        conn.execute(insert)
    
    tap = TapPostgres(config=modified_config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    
    for stream in tap_catalog.streams:
        if stream.stream and altered_table_name not in stream.stream:
            for metadata in stream.metadata.values():
                metadata.selected = False
        else:
            stream.replication_key = "updated_at"
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "updated_at"

    test_runner = TapTestRunner(
        tap_class=TapPostgres,
        config=modified_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()
    
    # Should only have the old record, recent record should be excluded by buffer
    records = test_runner.records[altered_table_name]
    assert len(records) == 1
    assert records[0]["data"] == "OldRecord"


def test_replication_key_buffer_seconds_disabled():
    """Test that when buffer is not set, all records are processed."""
    table_name = "test_replication_key_buffer_disabled"
    
    # Use config without buffer setting
    modified_config = copy.deepcopy(SAMPLE_CONFIG)
    
    engine = sa.create_engine(modified_config["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("data", sa.String()),
        sa.Column("updated_at", TIMESTAMP),
    )
    
    current_time = datetime.datetime.now(datetime.timezone.utc)
    
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        
        # Insert old record
        old_record_time = current_time - datetime.timedelta(seconds=60*10)  # 10 minutes ago
        insert = table.insert().values(
            data="OldRecord", 
            updated_at=old_record_time
        )
        conn.execute(insert)
        
        # Insert recent record
        recent_record_time = current_time - datetime.timedelta(seconds=60*4)  # 4 minutes ago
        insert = table.insert().values(
            data="RecentRecord", 
            updated_at=recent_record_time
        )
        conn.execute(insert)
    
    tap = TapPostgres(config=modified_config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    
    for stream in tap_catalog.streams:
        if stream.stream and altered_table_name not in stream.stream:
            for metadata in stream.metadata.values():
                metadata.selected = False
        else:
            stream.replication_key = "updated_at"
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "updated_at"

    test_runner = TapTestRunner(
        tap_class=TapPostgres,
        config=modified_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()
    
    # Should have both records when buffer is not set
    records = test_runner.records[altered_table_name]
    assert len(records) == 2
    record_data = {record["data"] for record in records}
    assert record_data == {"OldRecord", "RecentRecord"}


def test_replication_key_buffer_seconds_with_pacific_timezone():
    """Test that replication_key_buffer_seconds works correctly with Pacific timezone data."""
    table_name = "test_replication_key_buffer_pacific"
    
    # Create a config with a 300 second (5 minute) buffer
    modified_config = copy.deepcopy(SAMPLE_CONFIG)
    modified_config["replication_key_buffer_seconds"] = 300
    
    engine = sa.create_engine(modified_config["sqlalchemy_url"], future=True)

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("data", sa.String()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),  # Use TIMESTAMPTZ to preserve timezone
    )
    
    # Use Pacific timezone for test data
    pacific_tz = zoneinfo.ZoneInfo("US/Pacific")
    current_time_pacific = datetime.datetime.now(pacific_tz)
    
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        
        # Set PostgreSQL session timezone to Pacific to ensure data is stored in Pacific format
        conn.execute(sa.text("SET timezone = 'US/Pacific'"))

        # Insert record that's older than buffer (should be included)
        old_record_time_pacific = current_time_pacific - datetime.timedelta(seconds=60*6)  # 6 minutes ago
        insert = table.insert().values(
            data="OldPacificRecord", 
            updated_at=old_record_time_pacific
        )
        conn.execute(insert)
        
        # Insert record that's within buffer (should be excluded)
        recent_record_time_pacific = current_time_pacific - datetime.timedelta(seconds=60*4)  # 4 minutes ago
        insert = table.insert().values(
            data="RecentPacificRecord", 
            updated_at=recent_record_time_pacific
        )
        conn.execute(insert)
        
        # Verify the timestamps are stored with timezone information
        result = conn.execute(sa.text(f"SELECT data, updated_at FROM {table_name} ORDER BY updated_at")).fetchall()
        for row in result:
            data, timestamp = row
            # Verify the timestamp has timezone info when using TIMESTAMPTZ
            assert hasattr(timestamp, 'tzinfo'), f"Timestamp for {data} should be timezone-aware"
            assert timestamp.tzinfo is not None, f"Timestamp for {data} should have timezone info"
        
        # Verify the raw string representation contains Pacific timezone indicators
        raw_result = conn.execute(sa.text(f"SELECT data, updated_at::text FROM {table_name} ORDER BY updated_at")).fetchall()
        for row in raw_result:
            data, timestamp_str = row
            # Verify it contains Pacific timezone indicators
            pacific_indicators = ['-08', '-07', 'PST', 'PDT']
            assert any(tz_indicator in timestamp_str for tz_indicator in pacific_indicators), (
                f"Timestamp string '{timestamp_str}' should contain Pacific timezone indicators"
            )
    
    tap = TapPostgres(config=modified_config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    altered_table_name = f"{DB_SCHEMA_NAME}-{table_name}"
    
    for stream in tap_catalog.streams:
        if stream.stream and altered_table_name not in stream.stream:
            for metadata in stream.metadata.values():
                metadata.selected = False
        else:
            stream.replication_key = "updated_at"
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "updated_at"

    test_runner = TapTestRunner(
        tap_class=TapPostgres,
        config=modified_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()
    
    # Should only have the old record, recent record should be excluded by buffer
    records = test_runner.records[altered_table_name]
    assert len(records) == 1
    assert records[0]["data"] == "OldPacificRecord"

class TapTestReplicationKey(TapTestTemplate):
    name = "replication_key"
    table_name = TABLE_NAME

    def test(self):
        replication_key_test(self.tap, self.table_name)
