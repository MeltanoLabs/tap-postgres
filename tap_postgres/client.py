"""SQL client handling.

This includes PostgresStream and PostgresConnector.
"""
from __future__ import annotations

import datetime
import json
from json import JSONDecodeError
import select
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type, Union
import psycopg2
from psycopg2 import extras

import singer_sdk.helpers._typing
import sqlalchemy
from singer_sdk import SQLConnector, SQLStream
from singer_sdk import typing as th
from singer_sdk.helpers._typing import TypeConformanceLevel
from sqlalchemy import nullsfirst
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector

if TYPE_CHECKING:
    from sqlalchemy.dialects import postgresql

unpatched_conform = singer_sdk.helpers._typing._conform_primitive_property


def patched_conform(
    elem: Any,
    property_schema: dict,
) -> Any:
    """Overrides Singer SDK type conformance to prevent dates turning into datetimes.

    Converts a primitive (i.e. not object or array) to a json compatible type.

    Returns:
        The appropriate json compatible type.
    """
    if isinstance(elem, datetime.date):
        return elem.isoformat()
    return unpatched_conform(elem=elem, property_schema=property_schema)


singer_sdk.helpers._typing._conform_primitive_property = patched_conform


class PostgresConnector(SQLConnector):
    """Connects to the Postgres SQL source."""

    @staticmethod
    def to_jsonschema_type(
        sql_type: Union[
            str,
            sqlalchemy.types.TypeEngine,
            Type[sqlalchemy.types.TypeEngine],
            postgresql.ARRAY,
            Any,
        ]
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        Overidden from SQLConnector to correctly handle JSONB and Arrays.

        By default will call `typing.to_jsonschema_type()` for strings and SQLAlchemy
        types.

        Args
        ----
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises
        ------
            ValueError: If the type received could not be translated to jsonschema.

        Returns
        -------
            The JSON Schema representation of the provided type.

        """
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            type_name = type(sql_type).__name__

        if type_name is not None and type_name in ("JSONB", "JSON"):
            return th.ObjectType().type_dict

        if (
            type_name is not None
            and isinstance(sql_type, sqlalchemy.dialects.postgresql.ARRAY)
            and type_name == "ARRAY"
        ):
            array_type = PostgresConnector.sdk_typing_object(sql_type.item_type)
            return th.ArrayType(array_type).type_dict
        return PostgresConnector.sdk_typing_object(sql_type).type_dict

    @staticmethod
    def sdk_typing_object(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> (
        th.DateTimeType
        | th.NumberType
        | th.IntegerType
        | th.DateType
        | th.StringType
        | th.BooleanType
    ):
        """Return the JSON Schema dict that describes the sql type.

        Args
        ----
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises
        ------
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns
        -------
            A compatible JSON Schema type definition.

        """
        sqltype_lookup: dict[
            str,
            th.DateTimeType
            | th.NumberType
            | th.IntegerType
            | th.DateType
            | th.StringType
            | th.BooleanType,
        ] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking
            # precedence. If the SQL-provided type contains the type name on
            #  the left, the mapping will return the respective singer type.
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
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
            from_type, sqlalchemy.types.TypeEngine
        ):
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

    # JSONB Objects won't be selected without type_confomance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args
        ----
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields
        ------
            One dict per record.

        Raises
        ------
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
            query = query.order_by(nullsfirst(replication_key_col))

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.filter(replication_key_col >= start_val)

        for row in self.connector.connection.execute(query):
            yield dict(row)


class PostgresLogBasedStream(SQLStream):
    """Stream class for Postgres streams."""

    connector_class = PostgresConnector

    # JSONB Objects won't be selected without type_confomance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    replication_key = "_sdc_lsn"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.
        """

        status_interval = 5.0
        start_lsn = self.standardize_lsn(self.stream_state.get("bookmarks", {}).get(self.tap_stream_id, {}).get("_sdc_lsn", None))
        logical_replication_connection = self.logical_replication_connection()
        logical_replication_cursor = logical_replication_connection.cursor()

        logical_replication_cursor.start_replication(
            slot_name="tappostgres",
            decode=True,
            start_lsn=start_lsn,
            status_interval=status_interval,
            options={
                "format-version": 2,
                "add-tables": self.standardized_name,
            }
        )

        # Using scaffolding layout from: https://www.psycopg.org/docs/extras.html#psycopg2.extras.ReplicationCursor
        while True:
            message = logical_replication_cursor.read_message()
            if message:
                row = self.consume(message)
                if row:
                    yield row
            else:
                now = datetime.datetime.now()
                timeout = status_interval - (now - logical_replication_cursor.feedback_timestamp).total_seconds()
                try:
                    # If the timeout has passed and the cursor still has no new 
                    # messages, the sync has completed. 
                    if select.select([logical_replication_cursor], [], [], max(0, timeout))[0] == []:
                        break
                except InterruptedError:
                    pass

        logical_replication_cursor.close()
        logical_replication_connection.close()


    def consume(self, message) -> dict | None:
        try:
            message_payload = json.loads(message.payload)
        except JSONDecodeError:
            self.logger.warning("A message payload of %s could not be converted to JSON", message.payload)
            return

        row = {}

        upsert_actions = {"I","U"}
        delete_actions = {"D"}
        truncate_actions = {"T"}
        transaction_actions = {"B","C"}

        if message_payload["action"] in upsert_actions:
            for column in message_payload["columns"]:
                row.update({column["name"]: column["value"]})
            row.update({"_sdc_deleted_at": None})
            row.update({"_sdc_lsn": message.data_start})
        elif message_payload["action"] in delete_actions:
            for column in message_payload["identity"]:
                row.update({column["name"]: column["value"]})
            row.update({"_sdc_deleted_at": datetime.datetime.strftime(datetime.datetime.utcnow())})
            row.update({"_sdc_lsn": message.data_start})
        elif message_payload["action"] in truncate_actions:
            self.logger.warning("A message payload of %s (corresponding to a truncate action) could not be processed.", message.payload)
        elif message_payload["action"] in transaction_actions:
            self.logger.info("A message payload of %s (corresponding to a transaction beginning or commit) could not be processed.", message.payload)
        else:
            raise RuntimeError("A message payload of %s (corresponding to an unknown action type) could not be processed.", message.payload)

        return row

    
    def logical_replication_connection(self):
        return psycopg2.connect(
            f"dbname={self.config['database']} user={self.config['user']} password={self.config['password']} host={self.config['host']} port={self.config['port']}",
            application_name="tappostgres",
            connection_factory=extras.LogicalReplicationConnection,
            )
    

    @property
    def standardized_name(self):
        table_name=self.catalog_entry["table_name"]
        schema_name = None
        for metadata in self.catalog_entry["metadata"]:
            if metadata["breadcrumb"] == []:
                schema_name = metadata["metadata"]["schema-name"]
                break
        
        # TODO: escape special characters
        return f"{schema_name}.{table_name}"
    
    def standardize_lsn(self, lsn_string: str | None) -> int:
        if lsn_string is None or lsn_string is "":
            return 0
        components = lsn_string.split("/")
        if len(components) == 2:
            try:
                return (int(components[0], 16) << 32) + int(components[1], 16)
            except ValueError as e: # int() conversion had an invalid value
                raise RuntimeError(f"{lsn_string=} can't be converted") from e
        raise RuntimeError(f"{lsn_string=} can't be converted")

