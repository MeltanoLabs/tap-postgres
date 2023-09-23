"""SQL client handling.

This includes PostgresStream and PostgresConnector.
"""
from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type, Union

import psycopg2
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
    def to_jsonschema_type(
        self,
        sql_type: Union[
            str,
            sqlalchemy.types.TypeEngine,
            Type[sqlalchemy.types.TypeEngine],
            postgresql.ARRAY,
            Any,
        ],
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        Overidden from SQLConnector to correctly handle JSONB and Arrays.

        Also Overridden in order to call our instance method `sdk_typing_object()`
        instead of the static version

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
            array_type = self.sdk_typing_object(sql_type.item_type)
            return th.ArrayType(array_type).type_dict
        return self.sdk_typing_object(sql_type).type_dict

    def sdk_typing_object(
        self,
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
        if self.config["dates_as_string"] is True:
            sqltype_lookup["date"] = th.StringType()
            sqltype_lookup["datetime"] = th.StringType()
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

        with self.connector._connect() as con:
            for row in con.execute(query):
                yield dict(row)
