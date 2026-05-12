import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import TypeEngine

from tap_postgres.client import PostgresSQLToJSONSchema


@pytest.mark.parametrize(
    (
        "type_instance",
        "json_as_object",
        "schema",
    ),
    [
        pytest.param(
            postgresql.JSON(),
            True,
            {"type": ["object", "null"], "additionalProperties": True},
            id="json",
        ),
        pytest.param(
            postgresql.JSON(),
            False,
            {
                "type": ["string", "number", "integer", "array", "object", "boolean"],
                "additionalProperties": True,
            },
            id="json-any-type",
        ),
        pytest.param(
            postgresql.JSONB(),
            True,
            {"type": ["object", "null"], "additionalProperties": True},
            id="jsonb",
        ),
        pytest.param(
            postgresql.ARRAY(postgresql.JSON),
            True,
            {
                "type": "array",
                "items": {"type": ["object", "null"], "additionalProperties": True},
            },
            id="json[]",
        ),
        pytest.param(
            postgresql.ARRAY(postgresql.JSONB),
            True,
            {
                "type": "array",
                "items": {"type": ["object", "null"], "additionalProperties": True},
            },
            id="jsonb[]",
        ),
    ],
)
def test_json_conversion(type_instance: TypeEngine, json_as_object: bool, schema: dict):
    c = PostgresSQLToJSONSchema(dates_as_string=False, json_as_object=json_as_object)
    assert c.to_jsonschema(type_instance) == schema
