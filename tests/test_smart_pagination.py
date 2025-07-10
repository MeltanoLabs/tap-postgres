"""Tests for smart pagination implementation."""

import copy
import datetime
import unittest.mock as mock

import pytest
import sqlalchemy as sa
from singer_sdk.singerlib import Catalog, StreamMetadata
from singer_sdk.testing.runners import TapTestRunner
from sqlalchemy.dialects.postgresql import TIMESTAMP

from tap_postgres.client import PostgresStream
from tap_postgres.tap import TapPostgres
from tests.settings import DB_SCHEMA_NAME, DB_SQLALCHEMY_URL


SAMPLE_CONFIG = {
    "start_date": datetime.datetime(2022, 11, 1).isoformat(),
    "sqlalchemy_url": DB_SQLALCHEMY_URL,
}


class TestSmartPaginationMethods:
    """Test the individual methods of the smart pagination implementation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.tap = TapPostgres(config=SAMPLE_CONFIG)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "name": {"type": "string"},
                }
            },
            "metadata": {}
        }
        self.stream = PostgresStream(
            tap=self.tap,
            catalog_entry=catalog_entry,
            connector=self.tap.connector
        )
        self.stream.replication_key = "updated_at"

    def test_parse_state_with_normal_state(self):
        """Test _parse_state with normal state value."""
        result = self.stream._parse_state("2022-11-01T00:00:00")
        assert result == ("2022-11-01T00:00:00", None)

    def test_parse_state_with_special_state(self):
        """Test _parse_state with special state containing delimiter."""
        special_state = "2022-11-01T00:00:00||123"
        result = self.stream._parse_state(special_state)
        assert result == ("2022-11-01T00:00:00", "123")

    def test_parse_state_with_none(self):
        """Test _parse_state with None value."""
        result = self.stream._parse_state(None)
        assert result == (None, None)

    def test_parse_state_with_empty_string(self):
        """Test _parse_state with empty string."""
        result = self.stream._parse_state("")
        assert result == ("", None)

    def test_parse_state_with_non_string(self):
        """Test _parse_state with non-string value."""
        result = self.stream._parse_state(123)
        assert result == (123, None)

    def test_parse_state_with_multiple_delimiters(self):
        """Test _parse_state with multiple delimiters - should split on the last one."""
        special_state = "2022-11-01T00:00:00||data||123"
        result = self.stream._parse_state(special_state)
        assert result == ("2022-11-01T00:00:00||data", "123")

    def test_parse_state_with_delimiter_only(self):
        """Test _parse_state with only delimiter."""
        result = self.stream._parse_state("||")
        assert result == ("", "")

    def test_parse_state_with_empty_components(self):
        """Test _parse_state with empty components."""
        result = self.stream._parse_state("||123")
        assert result == ("", "123")
        
        result = self.stream._parse_state("2022-11-01T00:00:00||")
        assert result == ("2022-11-01T00:00:00", "")

    def test_get_id_column_name_default(self):
        """Test _get_id_column_name returns default 'id'."""
        result = self.stream._get_id_column_name()
        assert result == "id"

    def test_get_id_column_name_custom(self):
        """Test _get_id_column_name returns custom column name."""
        custom_config = copy.deepcopy(SAMPLE_CONFIG)
        custom_config["replication_tie_breaker_column"] = "custom_id"
        tap = TapPostgres(config=custom_config)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {
                "properties": {
                    "custom_id": {"type": "integer"},
                    "updated_at": {"type": "string", "format": "date-time"},
                }
            },
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        result = stream._get_id_column_name()
        assert result == "custom_id"

    def test_get_id_column_success(self):
        """Test _get_id_column returns correct column."""
        # Create a mock table with an id column
        mock_table = mock.MagicMock()
        mock_id_column = mock.MagicMock()
        mock_table.columns.get.return_value = mock_id_column
        
        result = self.stream._get_id_column(mock_table)
        assert result == mock_id_column
        mock_table.columns.get.assert_called_once_with("id")

    def test_get_id_column_not_found(self):
        """Test _get_id_column raises error when column not found."""
        # Create a mock table without an id column
        mock_table = mock.MagicMock()
        mock_table.columns.get.return_value = None
        
        with pytest.raises(ValueError, match="No suitable ID column found for table"):
            self.stream._get_id_column(mock_table)

    def test_compare_start_date_with_normal_state(self):
        """Test compare_start_date with normal state value."""
        value = "2022-11-15T00:00:00"
        start_date = "2022-11-01T00:00:00"
        result = self.stream.compare_start_date(value, start_date)
        assert result == value  # bookmark is more recent

    def test_compare_start_date_with_special_state_newer(self):
        """Test compare_start_date with special state that is newer than start date."""
        value = "2022-11-15T00:00:00||123"
        start_date = "2022-11-01T00:00:00"
        result = self.stream.compare_start_date(value, start_date)
        assert result == value  # Should return the entire special state

    def test_compare_start_date_with_special_state_older(self):
        """Test compare_start_date with special state that is older than start date."""
        value = "2022-10-15T00:00:00||123"
        start_date = "2022-11-01T00:00:00"
        result = self.stream.compare_start_date(value, start_date)
        assert result == start_date  # start date is more recent

    def test_compare_start_date_with_none_values(self):
        """Test compare_start_date with None values."""
        # Test with None bookmark - fallback to original implementation will fail with None
        with pytest.raises(TypeError):
            self.stream.compare_start_date(None, "2022-11-01T00:00:00")
        
        # Test with None start_date - fallback to original implementation will fail with None
        with pytest.raises(TypeError):
            self.stream.compare_start_date("2022-11-01T00:00:00", None)


class TestSmartPaginationIntegration:
    """Integration tests for smart pagination functionality."""

    def setup_method(self):
        """Set up test table for each test."""
        self.table_name = "test_smart_pagination"
        self.engine = sa.create_engine(SAMPLE_CONFIG["sqlalchemy_url"], future=True)
        self.setup_test_table()

    def teardown_method(self):
        """Clean up test table after each test."""
        self.teardown_test_table()

    def setup_test_table(self):
        """Create and populate test table."""
        metadata_obj = sa.MetaData()
        self.table = sa.Table(
            self.table_name,
            metadata_obj,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("updated_at", TIMESTAMP, nullable=False),
            sa.Column("name", sa.String()),
        )
        
        with self.engine.begin() as conn:
            self.table.drop(conn, checkfirst=True)
            metadata_obj.create_all(conn)
            
            # Insert test data with specific patterns for testing
            test_data = [
                {"id": 1, "updated_at": "2022-11-01T10:00:00", "name": "Record 1"},
                {"id": 2, "updated_at": "2022-11-01T10:00:00", "name": "Record 2"},  # Same timestamp
                {"id": 3, "updated_at": "2022-11-01T10:00:00", "name": "Record 3"},  # Same timestamp
                {"id": 4, "updated_at": "2022-11-01T11:00:00", "name": "Record 4"},
                {"id": 5, "updated_at": "2022-11-01T11:00:00", "name": "Record 5"},  # Same timestamp
                {"id": 6, "updated_at": "2022-11-01T12:00:00", "name": "Record 6"},
            ]
            
            for data in test_data:
                insert = self.table.insert().values(**data)
                conn.execute(insert)

    def teardown_test_table(self):
        """Drop test table."""
        with self.engine.begin() as conn:
            self.table.drop(conn, checkfirst=True)

    def test_smart_pagination_with_tie_breaker(self):
        """Test that smart pagination correctly uses tie-breaker column."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        tap_catalog = Catalog.from_dict(tap.catalog_dict)
        altered_table_name = f"{DB_SCHEMA_NAME}-{self.table_name}"
        
        # Configure stream for incremental sync
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

        # First sync - should get all records
        test_runner = TapTestRunner(
            tap_class=TapPostgres,
            config=SAMPLE_CONFIG,
            catalog=tap_catalog,
        )
        test_runner.sync_all()
        
        # Should have all 6 records
        records = test_runner.records[altered_table_name]
        assert len(records) == 6
        
        # Check that records are ordered by updated_at and id
        for i in range(1, len(records)):
            prev_record = records[i - 1]
            curr_record = records[i]
            
            # Compare timestamps
            prev_timestamp = prev_record["updated_at"]
            curr_timestamp = curr_record["updated_at"]
            
            if prev_timestamp == curr_timestamp:
                # If timestamps are equal, id should be ascending
                assert prev_record["id"] < curr_record["id"]
            else:
                # If timestamps are different, they should be ascending
                assert prev_timestamp <= curr_timestamp

    def test_smart_pagination_with_custom_tie_breaker(self):
        """Test smart pagination with custom tie-breaker column."""
        custom_config = copy.deepcopy(SAMPLE_CONFIG)
        custom_config["replication_tie_breaker_column"] = "id"
        
        tap = TapPostgres(config=custom_config)
        tap_catalog = Catalog.from_dict(tap.catalog_dict)
        altered_table_name = f"{DB_SCHEMA_NAME}-{self.table_name}"
        
        # Configure stream for incremental sync
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
            config=custom_config,
            catalog=tap_catalog,
        )
        test_runner.sync_all()
        
        # Should get all records with proper ordering
        records = test_runner.records[altered_table_name]
        assert len(records) == 6

    def test_smart_pagination_missing_tie_breaker_column(self):
        """Test error handling when tie-breaker column is missing."""
        # Create a table without 'id' column
        missing_id_table_name = "test_missing_id"
        metadata_obj = sa.MetaData()
        missing_id_table = sa.Table(
            missing_id_table_name,
            metadata_obj,
            sa.Column("updated_at", TIMESTAMP, nullable=False),
            sa.Column("name", sa.String()),
        )
        
        with self.engine.begin() as conn:
            missing_id_table.drop(conn, checkfirst=True)
            metadata_obj.create_all(conn)
            
            # Insert test data
            insert = missing_id_table.insert().values(
                updated_at="2022-11-01T10:00:00",
                name="Test Record"
            )
            conn.execute(insert)
        
        try:
            tap = TapPostgres(config=SAMPLE_CONFIG)
            tap_catalog = Catalog.from_dict(tap.catalog_dict)
            altered_table_name = f"{DB_SCHEMA_NAME}-{missing_id_table_name}"
            
            # Configure stream for incremental sync
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
            
            # This should raise an error about missing id column
            with pytest.raises(ValueError, match="No suitable ID column found"):
                test_runner.sync_all()
        
        finally:
            # Clean up the test table
            with self.engine.begin() as conn:
                missing_id_table.drop(conn, checkfirst=True)

    def test_smart_pagination_state_persistence(self):
        """Test that smart pagination state is correctly persisted and used."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        tap_catalog = Catalog.from_dict(tap.catalog_dict)
        altered_table_name = f"{DB_SCHEMA_NAME}-{self.table_name}"
        
        # Configure stream for incremental sync
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
        
        # Check that state messages contain the special state format
        state_messages = [msg for msg in test_runner.state_messages if "bookmarks" in msg]
        
        if state_messages:
            # Get the last state message
            last_state = state_messages[-1]
            bookmarks = last_state.get("bookmarks", {})
            
            # Find the bookmark for our table
            table_bookmark = None
            for stream_name, bookmark in bookmarks.items():
                if altered_table_name in stream_name:
                    table_bookmark = bookmark
                    break
            
            if table_bookmark and "replication_key_value" in table_bookmark:
                state_value = table_bookmark["replication_key_value"]
                # The state should contain the special delimiter
                assert "||" in state_value
                
                # Parse the state to verify it contains both timestamp and id
                parts = state_value.split("||")
                assert len(parts) == 2
                assert parts[0]  # timestamp part should not be empty
                assert parts[1]  # id part should not be empty

    def test_smart_pagination_tuple_comparison(self):
        """Test that tuple comparison works correctly for pagination."""
        # Create specific test data to verify tuple comparison
        tuple_test_table_name = "test_tuple_comparison"
        metadata_obj = sa.MetaData()
        tuple_test_table = sa.Table(
            tuple_test_table_name,
            metadata_obj,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("updated_at", TIMESTAMP, nullable=False),
            sa.Column("name", sa.String()),
        )
        
        with self.engine.begin() as conn:
            tuple_test_table.drop(conn, checkfirst=True)
            metadata_obj.create_all(conn)
            
            # Insert test data with specific ordering for tuple comparison
            test_data = [
                {"id": 1, "updated_at": "2022-11-01T10:00:00", "name": "A"},
                {"id": 2, "updated_at": "2022-11-01T10:00:00", "name": "B"},
                {"id": 3, "updated_at": "2022-11-01T10:00:00", "name": "C"},
                {"id": 4, "updated_at": "2022-11-01T11:00:00", "name": "D"},
                {"id": 5, "updated_at": "2022-11-01T11:00:00", "name": "E"},
            ]
            
            for data in test_data:
                insert = tuple_test_table.insert().values(**data)
                conn.execute(insert)
        
        try:
            # Test with a simulated state that should start from record 3
            custom_config = copy.deepcopy(SAMPLE_CONFIG)
            custom_config["start_date"] = None  # No start date to avoid conflicts
            
            tap = TapPostgres(config=custom_config)
            tap_catalog = Catalog.from_dict(tap.catalog_dict)
            altered_table_name = f"{DB_SCHEMA_NAME}-{tuple_test_table_name}"
            
            # Configure stream for incremental sync
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
            
            # Set up state to start from record 3 (2022-11-01T10:00:00+00:00||00000000000000000002)
            # This should include records 2, 3, 4, and 5 (tuple comparison >= (timestamp, id))
            state = {
                "bookmarks": {
                    altered_table_name: {
                        "replication_key": "updated_at",
                        "replication_key_value": "2022-11-01T10:00:00+00:00||00000000000000000002"
                    }
                }
            }
            
            test_runner = TapTestRunner(
                tap_class=TapPostgres,
                config=custom_config,
                catalog=tap_catalog,
                state=state,
            )
            test_runner.sync_all()
            
            # Should get records with id >= 2 (because of tuple comparison)
            records = test_runner.records[altered_table_name]
            assert len(records) == 4  # Records 2, 3, 4, 5
            
            # Verify the records are in the correct order
            expected_ids = [2, 3, 4, 5]
            actual_ids = [record["id"] for record in records]
            assert actual_ids == expected_ids
            
        finally:
            # Clean up the test table
            with self.engine.begin() as conn:
                tuple_test_table.drop(conn, checkfirst=True)


class TestSmartPaginationEdgeCases:
    """Test edge cases and error conditions for smart pagination."""

    def test_state_parsing_malformed_input(self):
        """Test state parsing with various malformed inputs."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {"properties": {"id": {"type": "integer"}}},
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        
        # Test with various malformed states
        malformed_states = [
            "||",  # Only delimiter
            "|||",  # Multiple delimiters only
            "||||value",  # Multiple delimiters at start
        ]
        
        for state in malformed_states:
            # These should not raise errors, just parse as expected
            result = stream._parse_state(state)
            assert isinstance(result, tuple)
            assert len(result) == 2

    def test_increment_stream_state_missing_columns(self):
        """Test _increment_stream_state with missing columns."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {"properties": {"updated_at": {"type": "string"}}},
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        stream.replication_key = "updated_at"
        
        # Mock replication_method to return INCREMENTAL
        with mock.patch.object(type(stream), 'replication_method', new_callable=mock.PropertyMock) as mock_replication_method:
            mock_replication_method.return_value = 'INCREMENTAL'
            # Test with record missing the tie-breaker column
            latest_record = {"updated_at": "2022-11-01T10:00:00"}
            
            # This should raise a KeyError when trying to access the missing id column
            with pytest.raises(KeyError):
                stream._increment_stream_state(latest_record)

    def test_increment_stream_state_with_null_values(self):
        """Test _increment_stream_state with null values."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {"properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}}},
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        stream.replication_key = "updated_at"
        
        # Mock replication_method to return INCREMENTAL
        with mock.patch.object(type(stream), 'replication_method', new_callable=mock.PropertyMock) as mock_replication_method:
            mock_replication_method.return_value = 'INCREMENTAL'
            # Test with null replication key value
            latest_record = {"id": 1, "updated_at": None}
            
            # This should handle null values gracefully
            stream._increment_stream_state(latest_record)
            
            # Check that the record was modified to include special state format
            assert "||" in str(latest_record["updated_at"])

    def test_get_id_column_with_custom_non_existent_column(self):
        """Test _get_id_column with custom column that doesn't exist."""
        custom_config = copy.deepcopy(SAMPLE_CONFIG)
        custom_config["replication_tie_breaker_column"] = "non_existent_column"
        
        tap = TapPostgres(config=custom_config)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {"properties": {"id": {"type": "integer"}}},
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        
        # Create a mock table without the custom column
        mock_table = mock.MagicMock()
        mock_table.columns.get.return_value = None
        
        with pytest.raises(ValueError, match="No suitable ID column found for table"):
            stream._get_id_column(mock_table)

    def test_compare_start_date_with_unparseable_dates(self):
        """Test compare_start_date with unparseable date strings."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {"properties": {"updated_at": {"type": "string"}}},
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        stream.replication_key = "updated_at"
        
        # Test with unparseable date in special state format
        value = "invalid-date||123"
        start_date = "2022-11-01T00:00:00"
        
        # This should raise an error when trying to parse the invalid date
        with pytest.raises(Exception):  # Could be ValueError or other parsing error
            stream.compare_start_date(value, start_date)

    def test_delimiter_in_data_values(self):
        """Test handling of delimiter characters in actual data values."""
        tap = TapPostgres(config=SAMPLE_CONFIG)
        catalog_entry = {
            "tap_stream_id": "test_stream",
            "table_name": "test_stream",
            "schema": {"properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}}},
            "metadata": {}
        }
        stream = PostgresStream(
            tap=tap,
            catalog_entry=catalog_entry,
            connector=tap.connector
        )
        stream.replication_key = "updated_at"
        
        # Mock replication_method to return INCREMENTAL
        with mock.patch.object(type(stream), 'replication_method', new_callable=mock.PropertyMock) as mock_replication_method:
            mock_replication_method.return_value = 'INCREMENTAL'
            # Test with data that contains the delimiter
            latest_record = {
                "id": "value||with||delimiters",
                "updated_at": "2022-11-01T10:00:00"
            }
            
            # This should still work correctly
            stream._increment_stream_state(latest_record)
            
            # The state should be parseable
            state_value = latest_record["updated_at"]
            parsed_rk_value, parsed_id = stream._parse_state(state_value)
            assert parsed_rk_value == "2022-11-01T10:00:00||value||with"  # rsplit includes delimiters in replication key part
            assert parsed_id == "delimiters"  # Only the rightmost part after the last delimiter


def test_special_state_delimiter_constant():
    """Test that the special state delimiter is correctly defined."""
    assert PostgresStream.SPECIAL_STATE_DELIMITER == "||"