"""SQL client handling.

This includes PostgresStream and PostgresConnector.
"""

from __future__ import annotations

import datetime
import json
import select
import typing
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Iterable, Mapping

import pendulum
import psycopg2
import singer_sdk.helpers._typing
import sqlalchemy as sa
from psycopg2 import extras
from singer_sdk import SQLConnector, SQLStream
from singer_sdk import typing as th
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

if TYPE_CHECKING:
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.types import TypeEngine


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
    if isinstance(elem, (datetime.datetime, pendulum.DateTime)):  # copied
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

    # Note super is static, we can get away with this because this is called once
    # and is luckily referenced via the instance of the class
    def to_jsonschema_type(  # type: ignore[override]
        self,
        sql_type: str | TypeEngine | type[TypeEngine] | postgresql.ARRAY | Any,
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        Overridden from SQLConnector to correctly handle JSONB and Arrays.

        Also Overridden in order to call our instance method `sdk_typing_object()`
        instead of the static version

        By default will call `typing.to_jsonschema_type()` for strings and SQLAlchemy
        types.

        Args:
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to jsonschema.

        Returns:
            The JSON Schema representation of the provided type.

        """
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type
        elif isinstance(sql_type, sa.types.TypeEngine):
            type_name = type(sql_type).__name__

        if (
            type_name is not None
            and isinstance(sql_type, sa.dialects.postgresql.ARRAY)
            and type_name == "ARRAY"
        ):
            array_type = self.sdk_typing_object(sql_type.item_type)
            return th.ArrayType(array_type).type_dict
        return self.sdk_typing_object(sql_type).type_dict

    def sdk_typing_object(
        self,
        from_type: str | TypeEngine | type[TypeEngine],
    ) -> (
        th.DateTimeType
        | th.NumberType
        | th.IntegerType
        | th.DateType
        | th.StringType
        | th.BooleanType
        | th.CustomType
    ):
        """Return the JSON Schema dict that describes the sql type.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.
        """
        # NOTE: This is an ordered mapping, with earlier mappings taking precedence. If
        # the SQL-provided type contains the type name on the left, the mapping will
        # return the respective singer type.
        # NOTE: jsonb and json should theoretically be th.AnyType().type_dict but that
        # causes problems down the line with an error like:
        # singer_sdk.helpers._typing.EmptySchemaTypeError: Could not detect type from
        # empty type_dict. Did you forget to define a property in the stream schema?
        sqltype_lookup: dict[
            str,
            th.DateTimeType
            | th.NumberType
            | th.IntegerType
            | th.DateType
            | th.StringType
            | th.BooleanType
            | th.CustomType,
        ] = {
            "jsonb": th.CustomType(
                {"type": ["string", "number", "integer", "array", "object", "boolean"]}
            ),
            "json": th.CustomType(
                {"type": ["string", "number", "integer", "array", "object", "boolean"]}
            ),
            "timestamp": th.DateTimeType(),
            "datetime": th.DateTimeType(),
            "date": th.DateType(),
            "int": th.IntegerType(),
            "numeric": th.NumberType(),
            "decimal": th.NumberType(),
            "double": th.NumberType(),
            "float": th.NumberType(),
            "string": th.StringType(),
            "text": th.StringType(),
            "char": th.StringType(),
            "bool": th.BooleanType(),
            "variant": th.StringType(),
        }
        if self.config["dates_as_string"] is True:
            sqltype_lookup["date"] = th.StringType()
            sqltype_lookup["datetime"] = th.StringType()
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sa.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(from_type, sa.types.TypeEngine):
            type_name = from_type.__name__
        else:
            raise ValueError(
                "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            )

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str

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

    # JSONB Objects won't be selected without type_conformance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

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
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )

        # pulling rows with only selected columns from stream
        selected_column_names = [k for k in self.get_selected_schema()["properties"]]
        table = self.connector.get_table(
            self.fully_qualified_name, column_names=selected_column_names
        )
        query = table.select()
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]

            # Nulls first because the default is to have nulls as the "highest" value
            # which incorrectly causes the tap to attempt to store null state.
            query = query.order_by(sa.nullsfirst(replication_key_col))

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.filter(replication_key_col >= start_val)

        with self.connector._connect() as con:
            for row in con.execute(query).mappings():
                yield dict(row)


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
        schema_dict = typing.cast(dict, self._singer_catalog_entry.schema.to_dict())
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
        context: dict | None = None,
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

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
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
                row = self.consume(message)
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

    def consume(self, message) -> dict | None:
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
                row.update({column["name"]: column["value"]})
            row.update({"_sdc_deleted_at": None})
            row.update({"_sdc_lsn": message.data_start})
        elif message_payload["action"] in delete_actions:
            for column in message_payload["identity"]:
                row.update({column["name"]: column["value"]})
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
