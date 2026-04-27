"""SQL client handling.

This includes PostgresStream and PostgresConnector.
"""

from __future__ import annotations

import datetime
import functools
import select
import sys
import typing as t
from types import MappingProxyType

import psycopg2
import sqlalchemy as sa
import sqlalchemy.types
from psycopg2 import extras
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.sql import SQLConnector, SQLStream
from singer_sdk.sql.connector import SQLToJSONSchema
from sqlalchemy.dialects import postgresql

from tap_postgres._wal_helpers import parse_wal_message

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from singer_sdk import Tap
    from singer_sdk.helpers.types import Context
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.sql.schema import Table
    from sqlalchemy.sql.selectable import Select

    from tap_postgres.connection_parameters import ConnectionParameters


_UPSERT_ACTIONS = {"I", "U"}
_DELETE_ACTIONS = {"D"}
_TRUNCATE_ACTIONS = {"T"}
_TRANSACTION_ACTIONS = {"B", "C"}
_NUMERIC_TYPES = ("int", "numeric", "decimal", "real", "double", "float", "bigint", "smallint")


def _now_utc() -> str:
    """Return the current UTC time as a string."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class PostgresSQLToJSONSchema(SQLToJSONSchema):
    """Custom SQL to JSON Schema conversion for Postgres."""

    def __init__(self, *, dates_as_string: bool, json_as_object: bool, **kwargs):
        """Initialize the SQL to JSON Schema converter."""
        super().__init__(**kwargs)
        self.dates_as_string = dates_as_string
        self.json_as_object = json_as_object

    @override
    @classmethod
    def from_config(cls, config: dict) -> PostgresSQLToJSONSchema:
        """Instantiate the SQL to JSON Schema converter from a config dictionary."""
        return cls(
            dates_as_string=config["dates_as_string"],
            json_as_object=config["json_as_object"],
        )

    @functools.singledispatchmethod
    def to_jsonschema(self, column_type: t.Any) -> dict:
        """Customize the JSON Schema for Postgres types."""
        return super().to_jsonschema(column_type)

    @to_jsonschema.register
    def array_to_jsonschema(self, column_type: postgresql.ARRAY) -> dict:
        """Override the default mapping for NUMERIC columns.

        For example, a scale of 4 translates to a multipleOf 0.0001.
        """
        return {
            "type": "array",
            "items": self.to_jsonschema(column_type.item_type),
        }

    @to_jsonschema.register
    def json_to_jsonschema(self, column_type: postgresql.JSON) -> dict:
        """Override the default mapping for JSON and JSONB columns."""
        if self.json_as_object:
            return {
                "type": ["object", "null"],
                "additionalProperties": True,
            }
        return {
            "type": ["string", "number", "integer", "array", "object", "boolean"],
            "additionalProperties": True,
        }

    @to_jsonschema.register
    def datetime_to_jsonschema(self, column_type: sqlalchemy.types.DateTime) -> dict:
        """Override the default mapping for DATETIME columns."""
        if self.dates_as_string:
            return {"type": ["string", "null"]}
        return super().datetime_to_jsonschema(column_type)

    @to_jsonschema.register
    def date_to_jsonschema(self, column_type: sqlalchemy.types.Date) -> dict:
        """Override the default mapping for DATE columns."""
        if self.dates_as_string:
            return {"type": ["string", "null"]}
        return super().date_to_jsonschema(column_type)

    @to_jsonschema.register
    def hstore_to_jsonschema(self, column_type: postgresql.HSTORE) -> dict:
        """Override the default mapping for HSTORE columns."""
        return {
            "type": ["object", "null"],
            "additionalProperties": True,
        }


class PostgresConnector(SQLConnector):
    """Connects to the Postgres SQL source."""

    sql_to_jsonschema_converter = PostgresSQLToJSONSchema

    def __init__(
        self,
        config: dict | None = None,
        sqlalchemy_url: str | None = None,
    ) -> None:
        """Initialize the SQL connector.

        Args:
          config: The parent tap or target object's config.
          sqlalchemy_url: Optional URL for the connection.

        """
        # Dates in postgres don't all convert to python datetime objects, so we
        # need to register a custom type caster to convert these to a string
        # See https://www.psycopg.org/psycopg3/docs/advanced/adapt.html#example-handling-infinity-date # noqa: E501
        # For more information
        if config is not None and config["dates_as_string"] is True:
            string_dates = psycopg2.extensions.new_type(
                (1082, 1114, 1184), "STRING_DATES", psycopg2.STRING
            )
            string_date_arrays = psycopg2.extensions.new_array_type(
                (1182, 1115, 1188), "STRING_DATE_ARRAYS[]", psycopg2.STRING
            )
            psycopg2.extensions.register_type(string_dates)
            psycopg2.extensions.register_type(string_date_arrays)

        super().__init__(config=config, sqlalchemy_url=sqlalchemy_url)

    @classmethod
    def from_connection_parameters(
        cls,
        config: Mapping[str, t.Any],
        connection_parameters: ConnectionParameters,
    ) -> PostgresConnector:
        """Instantiate the connector from connection parameters.

        Args:
            config: Tap config dictionary.
            connection_parameters: Connection parameters object.

        Returns:
            An instance of PostgresConnector.
        """
        return cls(
            config=dict(config),
            sqlalchemy_url=connection_parameters.render_as_sqlalchemy_url(),
        )

    @override
    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        """Return a list of schema names in DB, or overrides with user-provided values.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        if "filter_schemas" in self.config and len(self.config["filter_schemas"]) != 0:
            return self.config["filter_schemas"]
        return super().get_schema_names(engine, inspected)


class PostgresStream(SQLStream):
    """Stream class for Postgres streams."""

    connector_class = PostgresConnector
    supports_nulls_first = True

    # JSONB Objects won't be selected without type_conformance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def max_record_count(self) -> int | None:
        """Return the maximum number of records to fetch in a single query."""
        return self.config.get("max_record_count")

    @override
    def apply_query_limit(self, query: Select) -> Select:
        query = super().apply_query_limit(query)

        if max_records := self.max_record_count():
            query = query.limit(max_records)

        return query

    @override
    def apply_query_filters(
        self,
        query: Select,
        table: Table,
        *,
        context: Context | None = None,
    ) -> Select:
        """Apply query filters to the query."""
        query = super().apply_query_filters(query, table, context=context)
        stream_options = self.config.get("stream_options", {}).get(self.name, {})
        if clauses := stream_options.get("custom_where_clauses"):
            query = query.where(*(sa.text(clause.strip()) for clause in clauses))
        return query


class PostgresLogBasedStream(SQLStream):
    """Stream class for Postgres log-based streams."""

    connector_class = PostgresConnector

    # JSONB Objects won't be selected without type_conformance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    replication_key = "_sdc_lsn"
    is_sorted = True

    connection_parameters: ConnectionParameters

    def __init__(
        self,
        tap: Tap,
        catalog_entry: dict,
        connection_parameters: ConnectionParameters,
        connector: SQLConnector | None = None,
    ) -> None:
        """Initialize Postgres log-based stream."""
        self.connection_parameters = connection_parameters

        super().__init__(tap, catalog_entry, connector)

    @property
    def config(self) -> Mapping[str, t.Any]:
        """Return a read-only config dictionary."""
        return MappingProxyType(self._config)

    @override
    @functools.cached_property
    def effective_schema(self) -> dict:
        """Override schema for log-based replication adding _sdc columns."""
        schema_dict = super().effective_schema

        for property in schema_dict["properties"].values():
            if isinstance(property["type"], list):
                property["type"].append("null")
            else:
                property["type"] = [property["type"], "null"]

        if "required" in schema_dict:
            schema_dict.pop("required")

        schema_dict["properties"].update({"_sdc_deleted_at": {"type": ["string"]}})
        schema_dict["properties"].update({"_sdc_lsn": {"type": ["integer"]}})

        return schema_dict

    @override
    def _increment_stream_state(
        self,
        latest_record: dict[str, t.Any],
        *,
        context: Context | None = None,
    ) -> None:
        """Update state bookmark with max-forward-only LSN advancement.

        The base class only advances bookmarks for INCREMENTAL streams.
        LOG_BASED streams need this override to track the replication position.

        WAL records are delivered in LSN order overall, but records near
        transaction boundaries can arrive with LSN values slightly below the
        stored bookmark (e.g. when start_replication resumes mid-transaction).
        We silently skip those rather than crashing with InvalidStreamSortException.
        """
        if not latest_record or not self.replication_key:
            return

        state_dict = self.get_context_state(context)
        new_value = latest_record.get(self.replication_key)  # ty:ignore[invalid-argument-type]
        if new_value is None:
            return

        old_value = state_dict.get("replication_key_value")
        if old_value is None or new_value >= old_value:
            state_dict["replication_key"] = self.replication_key
            state_dict["replication_key_value"] = new_value

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, t.Any]]:
        """Iterate records for this LOG_BASED stream.

        Under the single-connection model (default), the first call into get_records()
        across any LOG_BASED stream triggers the shared WAL reader, which emits records
        for *all* selected LOG_BASED streams via their ``emit_record()`` adapter —
        bypassing this generator entirely. Subsequent sibling calls are no-ops.

        The yield-nothing behavior is intentional: ``Stream._sync_records()`` iterates
        whatever this returns, so an empty iterable produces zero additional record messages
        from the SDK's per-stream loop, while leaving SCHEMA emission, metrics,
        and state finalization intact.

        Under the legacy per-stream model, falls back to the original WAL reading loop.
        """
        if not self._tap.config.get("log_based_single_connection", True):
            yield from self._get_records_per_stream(context)
            return

        if not self._tap._shared_wal_run_completed:
            self._tap._sync_log_based_streams_shared()
            self._tap._shared_wal_run_completed = True

        return

    def _get_records_per_stream(self, context: Context | None) -> Iterable[dict[str, t.Any]]:
        """Return a generator of row-type dictionary objects.

        Runs a long-lived replication session (up to
        ``replication_max_run_seconds``, default 600 s) so the tap can drain
        large WAL backlogs in a single sync.  Sends periodic flush feedback
        while yielding records so the slot releases retained WAL incrementally.

        After the loop ends -- either because no data messages arrived for
        ``replication_idle_exit_seconds`` (default 60 s) or the time budget is
        exhausted -- the slot is advanced to the current WAL tip to prevent
        unbounded WAL retention.
        """
        status_interval = 10
        max_run_seconds = self.config["replication_max_run_seconds"]
        idle_exit_seconds = self.config["replication_idle_exit_seconds"]
        feedback_interval = 30

        start_lsn = self.get_starting_replication_key_value(context=context)
        if start_lsn is None:
            start_lsn = 0

        logical_replication_connection = self.logical_replication_connection()
        logical_replication_cursor = logical_replication_connection.cursor()

        logical_replication_cursor.send_feedback(flush_lsn=start_lsn)

        replication_slot_name = self.config["replication_slot_name"]

        logical_replication_cursor.start_replication(
            slot_name=replication_slot_name,
            decode=True,
            start_lsn=start_lsn,
            status_interval=status_interval,
            options={
                "format-version": 2,
                "include-transaction": False,
                "add-tables": self.fully_qualified_name,
            },
        )

        run_start = datetime.datetime.now()
        last_data_message = run_start
        last_feedback_time = run_start
        records_yielded = 0

        while True:
            now = datetime.datetime.now()
            elapsed = (now - run_start).total_seconds()
            if elapsed > max_run_seconds:
                self.logger.info(
                    "Reached max run time of %d seconds (%d records yielded)",
                    max_run_seconds,
                    records_yielded,
                )
                break

            message = logical_replication_cursor.read_message()
            if message:
                last_data_message = datetime.datetime.now()
                payload = parse_wal_message(message.payload, logical_replication_cursor)
                if payload is not None:
                    row = self.consume(payload, message.data_start)
                    if row:
                        records_yielded += 1
                        yield row
                    if (
                        datetime.datetime.now() - last_feedback_time
                    ).total_seconds() >= feedback_interval:
                        try:
                            logical_replication_cursor.send_feedback(
                                flush_lsn=message.data_start,
                            )
                            last_feedback_time = datetime.datetime.now()
                        except Exception:
                            pass
                else:
                    self.logger.warning(
                        "A message payload of %s could not be converted to JSON",
                        message.payload,
                    )
                continue

            try:
                ready = select.select(
                    [logical_replication_cursor],
                    [],
                    [],
                    1.0,
                )[0]
            except InterruptedError:
                ready = [logical_replication_cursor]

            if not ready:
                data_idle = (datetime.datetime.now() - last_data_message).total_seconds()
                if data_idle >= idle_exit_seconds:
                    self.logger.info(
                        "No data messages for %.0f seconds, ending sync "
                        "(%d records yielded in %.0f seconds)",
                        data_idle,
                        records_yielded,
                        elapsed,
                    )
                    break

        self._advance_slot_and_state(
            logical_replication_cursor,
            start_lsn,
            context,
        )

        logical_replication_cursor.close()
        logical_replication_connection.close()

    def emit_record(self, record: dict, *, context: Context | None = None) -> None:
        """Emit one record as a Singer RECORD message and advance state.

        This is meant to decouple the shared WAL reader ``SingleConnectionWALReader``
        from singer-sdk's per-record internals. It does the following, in order:

        1. stream map transformation (aliasing, field renames, filters)
        2. type conformance according to ``TYPE_CONFORMANCE_LEVEL``
        3. emission of one or more RECORD messages to stdout
        4. advancement of stream's replication bookmark based on ``record["_sdc_lsn"]``

        STATE message emission is *not* done here; that's the caller's responsibility.
        """
        self._write_record_message(record)
        self._increment_stream_state(record, context=context)

    def _advance_slot_and_state(
        self,
        replication_cursor: extras.ReplicationCursor,
        start_lsn: int,
        context: Context | None,
    ) -> None:
        """Advance the replication slot and bookmark to the current WAL tip.

        When ``add-tables`` filters out most WAL records, the slot's confirmed
        flush position can fall far behind the actual WAL tip, causing
        PostgreSQL to retain gigabytes of WAL that will never be consumed.

        This method queries the server for its current WAL flush position on a
        separate (regular) connection and, if it is ahead of ``start_lsn``:

        1. Sends ``send_feedback`` on the replication cursor so the slot can
           release retained WAL.
        2. Updates ``replication_key_value`` in the stream state so the next
           sync resumes from the advanced position rather than re-scanning the
           same WAL segment.

        Records between ``start_lsn`` and the new position for *other* tables
        are irrelevant (filtered by ``add-tables``).  Any matching records for
        *this* table that fell within the scanned window were already yielded
        by ``get_records``; records beyond the scan window will be picked up
        from the new, advanced position on the next run.
        """
        flush_lsn: int | None = None

        # Prefer the wal_end reported by the server during the replication
        # session (set from keepalive or data messages).
        try:
            wal_end = getattr(replication_cursor, "wal_end", 0) or 0
            if wal_end > start_lsn:
                flush_lsn = wal_end
        except Exception:
            pass

        # Fallback: query the server directly for the current WAL position.
        if not flush_lsn or flush_lsn <= start_lsn:
            flush_lsn = self._query_current_wal_lsn()

        if not flush_lsn or flush_lsn <= start_lsn:
            return

        try:
            replication_cursor.send_feedback(flush_lsn=flush_lsn)
            self.logger.info(
                "Advanced replication slot confirmed position from %d to %d (delta %.2f MB)",
                start_lsn,
                flush_lsn,
                (flush_lsn - start_lsn) / (1024 * 1024),
            )
        except Exception as exc:
            self.logger.warning("Failed to send final slot feedback: %s", exc)
            return

        state_dict = self.get_context_state(context)
        state_dict["replication_key"] = self.replication_key
        state_dict["replication_key_value"] = flush_lsn

    def _query_current_wal_lsn(self) -> int | None:
        """Query pg_current_wal_flush_lsn() and return the result as an int."""
        try:
            conn = psycopg2.connect(
                self.connection_parameters.render_as_psycopg2_dsn(),
            )
            try:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_current_wal_flush_lsn()")
                    row = cur.fetchone()
                    if row is None:
                        return None
                    lsn_str = row[0]  # e.g. '6/4A3B2C10'
                    hi, lo = lsn_str.split("/")
                    return (int(hi, 16) << 32) + int(lo, 16)
            finally:
                conn.close()
        except Exception as exc:
            self.logger.warning("Could not query current WAL LSN: %s", exc)
            return None

    def consume(self, payload: dict, lsn: int) -> dict | None:
        """Build a Singer row dict from a parsed wal2json payload.

        Returns:
            A dict suitable for emission as a RECORD, or None for non-data messages
            (truncate, transaction begin/commit, unrecognized actions that were logged)

        This method is cursor-independent! ``text[]`` values in ``payload`` are expected
        to have been pre-parsed into Python lists (see ``_pre_parse_text_arrays``).
        ``lsn`` is the ``message.data_start`` value from the replication cursor,
        used both as the ``_sdc_lsn`` column and as the state bookmark.
        """
        action = payload["action"]

        if action in _UPSERT_ACTIONS:
            row = {
                column["name"]: self._parse_column_value(column) for column in payload["columns"]
            }
            row["_sdc_deleted_at"] = None
            row["_sdc_lsn"] = lsn
            return row

        if action in _DELETE_ACTIONS:
            row = {
                column["name"]: self._parse_column_value(column) for column in payload["identity"]
            }
            row["_sdc_deleted_at"] = _now_utc()
            row["_sdc_lsn"] = lsn
            return row

        if action in _TRUNCATE_ACTIONS:
            self.logger.debug(
                "A message payload of %s (corresponding to a truncate action) "
                "could not be processed.",
                payload,
            )
            return None

        if action in _TRANSACTION_ACTIONS:
            self.logger.debug(
                "A message payload of %s (corresponding to a transaction begin "
                "or commit) could not be processed.",
                payload,
            )
            return None

        raise RuntimeError(
            f"A message payload of {payload!r} (corresponding to an unknown "
            f"action type {action!r}) could not be processed."
        )

    def _parse_column_value(self, column: dict) -> t.Any:
        """Parse a single wal2json column dict into a Python value.

        Handles nullability, numeric-empty-string, and ``text[]``. A string value here
        is a programming error, but we handle it gracefully with a last-resort parse.
        """
        value = column.get("value")
        if value is None:
            return None

        column_type = column.get("type", "")

        if column_type == "text[]":
            if isinstance(value, list):
                return value
            # fallback, reachable only if a caller forgot to call _pre_parse_text_arrays
            # STRINGARRAY with cursor=None works for UTF-8 connections
            # which is the majority case
            self.logger.warning(
                "Encountered unparsed text[] value in _parse_column_value; falling back "
                "to cursor-less STRINGARRAY parse. This indicates a missing call "
                "to _pre_parse_text_arrays()."
            )
            return psycopg2.extensions.STRINGARRAY(value, None)

        if value == "" and any(numeric_type in column_type for numeric_type in _NUMERIC_TYPES):
            return None

        return value

    def logical_replication_connection(self) -> extras.LogicalReplicationConnection:
        """A logical replication connection to the database.

        Uses a direct psycopg2 implementation rather than through sqlalchemy.
        """
        connection_parameters = self.connection_parameters

        return psycopg2.connect(
            connection_parameters.render_as_psycopg2_dsn(),
            connection_factory=extras.LogicalReplicationConnection,
        )
