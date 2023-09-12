"""SQL client handling.

This includes PostgresStream and PostgresConnector.
"""

from __future__ import annotations

import datetime
import functools
import json
import select
import typing as t
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Iterable, Mapping

import psycopg2
import singer_sdk.helpers._typing
import sqlalchemy as sa
import sqlalchemy.types
from psycopg2 import extras
from singer_sdk import SQLConnector, SQLStream
from singer_sdk.connectors.sql import SQLToJSONSchema
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from sqlalchemy.dialects import postgresql

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class PostgresSQLToJSONSchema(SQLToJSONSchema):
    """Custom SQL to JSON Schema conversion for Postgres."""

    def __init__(self, dates_as_string: bool, *args, **kwargs):
        """Initialize the SQL to JSON Schema converter."""
        super().__init__(*args, **kwargs)
        self.dates_as_string = dates_as_string

    @SQLToJSONSchema.to_jsonschema.register  # type: ignore[attr-defined]
    def array_to_jsonschema(self, column_type: postgresql.ARRAY) -> dict:
        """Override the default mapping for NUMERIC columns.

        For example, a scale of 4 translates to a multipleOf 0.0001.
        """
        return {
            "type": "array",
            "items": self.to_jsonschema(column_type.item_type),
        }

    @SQLToJSONSchema.to_jsonschema.register  # type: ignore[attr-defined]
    def json_to_jsonschema(self, column_type: postgresql.JSON) -> dict:
        """Override the default mapping for JSON and JSONB columns."""
        return {"type": ["string", "number", "integer", "array", "object", "boolean"]}

    @SQLToJSONSchema.to_jsonschema.register  # type: ignore[attr-defined]
    def datetime_to_jsonschema(self, column_type: sqlalchemy.types.DateTime) -> dict:
        """Override the default mapping for DATETIME columns."""
        if self.dates_as_string:
            return {"type": ["string", "null"]}
        return super().datetime_to_jsonschema(column_type)

    @SQLToJSONSchema.to_jsonschema.register  # type: ignore[attr-defined]
    def date_to_jsonschema(self, column_type: sqlalchemy.types.Date) -> dict:
        """Override the default mapping for DATE columns."""
        if self.dates_as_string:
            return {"type": ["string", "null"]}
        return super().date_to_jsonschema(column_type)


def patched_conform(
    elem: Any,
    property_schema: dict,
) -> Any:
    """Overrides Singer SDK type conformance.

    Most logic here is from singer_sdk.helpers._typing._conform_primitive_property, as
    marked by "# copied". This is a full override rather than calling the "super"
    because the final piece of logic in the super `if is_boolean_type(property_schema):`
    is flawed. is_boolean_type will return True if the schema contains a boolean
    anywhere. Therefore, a jsonschema type like ["boolean", "integer"] will return true
    and will have its values coerced to either True or False. In practice, this occurs
    for columns with JSONB type: no guarantees can be made about their data, so the
    schema has every possible data type, including boolean. Without this override, all
    JSONB columns would be coerced to True or False.

    Modifications:
     - prevent dates from turning into datetimes.
     - prevent collapsing values to booleans. (discussed above)

    Converts a primitive (i.e. not object or array) to a json compatible type.

    Returns:
        The appropriate json compatible type.
    """
    if isinstance(elem, datetime.date):  # not copied, original logic
        return elem.isoformat()
    if isinstance(elem, (datetime.datetime,)):  # copied
        return singer_sdk.helpers._typing.to_json_compatible(elem)
    if isinstance(elem, datetime.timedelta):  # copied
        epoch = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
        timedelta_from_epoch = epoch + elem
        if timedelta_from_epoch.tzinfo is None:
            timedelta_from_epoch = timedelta_from_epoch.replace(
                tzinfo=datetime.timezone.utc
            )
        return timedelta_from_epoch.isoformat()
    if isinstance(elem, datetime.time):  # copied
        return str(elem)
    if isinstance(elem, bytes):  # copied, modified to import is_boolean_type
        # for BIT value, treat 0 as False and anything else as True
        # Will only due this for booleans, not `bytea` data.
        return (
            elem != b"\x00"
            if singer_sdk.helpers._typing.is_boolean_type(property_schema)
            else elem.hex()
        )
    return elem


singer_sdk.helpers._typing._conform_primitive_property = patched_conform


class PostgresConnector(SQLConnector):
    """Connects to the Postgres SQL source."""

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

    @functools.cached_property
    def sql_to_jsonschema(self):
        """Return a mapping of SQL types to JSON Schema types."""
        return PostgresSQLToJSONSchema(dates_as_string=self.config["dates_as_string"])

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

    # Get records from stream
    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            order_by = (
                sa.nulls_first(replication_key_col.asc())
                if self.supports_nulls_first
                else replication_key_col.asc()
            )
            query = query.order_by(order_by)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        if self.ABORT_AT_RECORD_COUNT is not None:
            # Limit record count to one greater than the abort threshold. This ensures
            # `MaxRecordsLimitException` exception is properly raised by caller
            # `Stream._sync_records()` if more records are available than can be
            # processed.
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        if self.max_record_count():
            query = query.limit(self.max_record_count())

        with self.connector._connect() as conn:
            for record in conn.execute(query).mappings():
                # TODO: Standardize record mapping type
                # https://github.com/meltano/sdk/issues/2096
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record


class PostgresLogBasedStream(SQLStream):
    """Stream class for Postgres log-based streams."""

    connector_class = PostgresConnector

    # JSONB Objects won't be selected without type_confomance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    replication_key = "_sdc_lsn"

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a read-only config dictionary."""
        return MappingProxyType(self._config)

    @cached_property
    def schema(self) -> dict:
        """Override schema for log-based replication adding _sdc columns."""
        schema_dict = t.cast(dict, self._singer_catalog_entry.schema.to_dict())
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

    def _increment_stream_state(
        self,
        latest_record: dict[str, Any],
        *,
        context: Context | None = None,
    ) -> None:
        """Update state of stream or partition with data from the provided record.

        The default implementation does not advance any bookmarks unless
        `self.replication_method == 'INCREMENTAL'`. For us, `self.replication_method ==
        'LOG_BASED'`, so an override is required.
        """
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if latest_record:  # This is the only line that has been overridden.
            if not self.replication_key:
                msg = (
                    f"Could not detect replication key for '{self.name}' "
                    f"stream(replication method={self.replication_method})"
                )
                raise ValueError(msg)
            treat_as_sorted = self.is_sorted()
            if not treat_as_sorted and self.state_partitioning_keys is not None:
                # Streams with custom state partitioning are not resumable.
                treat_as_sorted = False
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record=latest_record,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects."""
        status_interval = 5.0  # if no records in 5 seconds the tap can exit
        start_lsn = self.get_starting_replication_key_value(context=context)
        if start_lsn is None:
            start_lsn = 0
        logical_replication_connection = self.logical_replication_connection()
        logical_replication_cursor = logical_replication_connection.cursor()

        # Flush logs from the previous sync. send_feedback() will only flush LSNs before
        # the value of flush_lsn, not including the value of flush_lsn, so this is safe
        # even though we still want logs with an LSN == start_lsn.
        logical_replication_cursor.send_feedback(flush_lsn=start_lsn)

        logical_replication_cursor.start_replication(
            slot_name="tappostgres",
            decode=True,
            start_lsn=start_lsn,
            status_interval=status_interval,
            options={
                "format-version": 2,
                "include-transaction": False,
                "add-tables": self.fully_qualified_name,
            },
        )

        # Using scaffolding layout from:
        # https://www.psycopg.org/docs/extras.html#psycopg2.extras.ReplicationCursor
        while True:
            message = logical_replication_cursor.read_message()
            if message:
                row = self.consume(message, logical_replication_cursor)
                if row:
                    yield row
            else:
                timeout = (
                    status_interval
                    - (
                        datetime.datetime.now()
                        - logical_replication_cursor.feedback_timestamp
                    ).total_seconds()
                )
                try:
                    # If the timeout has passed and the cursor still has no new
                    # messages, the sync has completed.
                    if (
                        select.select(
                            [logical_replication_cursor], [], [], max(0, timeout)
                        )[0]
                        == []
                    ):
                        break
                except InterruptedError:
                    pass

        logical_replication_cursor.close()
        logical_replication_connection.close()

    def consume(self, message, cursor) -> dict | None:
        """Ingest WAL message."""
        try:
            message_payload = json.loads(message.payload)
        except json.JSONDecodeError:
            self.logger.warning(
                "A message payload of %s could not be converted to JSON",
                message.payload,
            )
            return {}

        row = {}

        upsert_actions = {"I", "U"}
        delete_actions = {"D"}
        truncate_actions = {"T"}
        transaction_actions = {"B", "C"}

        if message_payload["action"] in upsert_actions:
            for column in message_payload["columns"]:
                row.update({column["name"]: self._parse_column_value(column, cursor)})
            row.update({"_sdc_deleted_at": None})
            row.update({"_sdc_lsn": message.data_start})
        elif message_payload["action"] in delete_actions:
            for column in message_payload["identity"]:
                row.update({column["name"]: self._parse_column_value(column, cursor)})
            row.update(
                {
                    "_sdc_deleted_at": datetime.datetime.utcnow().strftime(
                        r"%Y-%m-%dT%H:%M:%SZ"
                    )
                }
            )
            row.update({"_sdc_lsn": message.data_start})
        elif message_payload["action"] in truncate_actions:
            self.logger.debug(
                (
                    "A message payload of %s (corresponding to a truncate action) "
                    "could not be processed."
                ),
                message.payload,
            )
        elif message_payload["action"] in transaction_actions:
            self.logger.debug(
                (
                    "A message payload of %s (corresponding to a transaction beginning "
                    "or commit) could not be processed."
                ),
                message.payload,
            )
        else:
            raise RuntimeError(
                (
                    "A message payload of %s (corresponding to an unknown action type) "
                    "could not be processed."
                ),
                message.payload,
            )

        return row

    def _parse_column_value(self, column, cursor):
        # When using log based replication, the wal2json output for columns of
        # array types returns a string encoded in sql format, e.g. '{a,b}'
        # https://github.com/eulerto/wal2json/issues/221#issuecomment-1025143441
        if column["type"] == "text[]":
            return psycopg2.extensions.STRINGARRAY(column["value"], cursor)

        return column["value"]

    def logical_replication_connection(self):
        """A logical replication connection to the database.

        Uses a direct psycopg2 implementation rather than through sqlalchemy.
        """
        connection_string = (
            f"dbname={self.config['database']} user={self.config['user']} password="
            f"{self.config['password']} host={self.config['host']} port="
            f"{self.config['port']}"
        )
        return psycopg2.connect(
            connection_string,
            application_name="tap_postgres",
            connection_factory=extras.LogicalReplicationConnection,
        )

    # TODO: Make this change upstream in the SDK?
    # I'm not sure if in general SQL databases don't guarantee order of records log
    # replication, but at least Postgres does not.
    def is_sorted(self) -> bool:  # type: ignore[override]
        """Return True if the stream is sorted by the replication key."""
        return self.replication_method == REPLICATION_INCREMENTAL
