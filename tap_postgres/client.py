"""SQL client handling.

This includes PostgresStream and PostgresConnector.
"""
from __future__ import annotations

import sqlalchemy
import sys
from typing import Optional, Iterable, Any, Dict, Union, Type
from typing import Generic, Mapping, TypeVar, Union, cast
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, JSONB
from sqlalchemy.types import TIMESTAMP
from singer_sdk import typing as th

from singer_sdk import SQLConnector, SQLStream


class PostgresConnector(SQLConnector):
    """Connects to the Postgres SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return config['sqlalchemy_url']
    
    @staticmethod
    def to_jsonschema_type(
        sql_type: Union[
            str, sqlalchemy.types.TypeEngine, Type[sqlalchemy.types.TypeEngine], sqlalchemy.dialects.postgresql.ARRAY, Any
        ]
        ) -> dict:
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            type_name = type(sql_type).__name__

        if type_name is not None and type_name == "JSONB":
            return th.ObjectType().type_dict
        
        if type_name is not None and isinstance(sql_type, sqlalchemy.dialects.postgresql.ARRAY) and type_name == "ARRAY":
            array_type = PostgresConnector.sdk_typing_object(sql_type.item_type)
            return th.ArrayType(array_type).type_dict
        return PostgresConnector.sdk_typing_object(sql_type).type_dict


    @staticmethod
    def sdk_typing_object(
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> (th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType) :
        """Return the JSON Schema dict that describes the sql type.
    
        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.
    
        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.
    
        Returns:
            A compatible JSON Schema type definition.
        """
        sqltype_lookup: dict[str, th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
            #       If the SQL-provided type contains the type name on the left, the mapping
            #       will return the respective singer type.
            "timestamp": th.DateTimeType(),
            "datetime": th.DateTimeType(),
            "date": th.DateType(),
            "int": th.IntegerType(),
            "number": th.NumberType(),
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
            raise ValueError("Expected `str` or a SQLAlchemy `TypeEngine` object or type.")
    
        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type
    
        return sqltype_lookup["string"]  # safe failover to str



class PostgresStream(SQLStream):
    """Stream class for Postgres streams."""

    connector_class = PostgresConnector

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        yield from super().get_records(partition)
