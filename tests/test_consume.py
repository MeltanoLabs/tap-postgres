"""Unit tests for parsing helpers and ``PostgresLogBasedStream.consume()``.

No Postgres needed. Hand-crafted wal2json payloads cover the action-type branches
in ``consume()`` and the recovery paths in ``parse_wal_message``.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema

from tap_postgres._wal_helpers import parse_wal_message
from tap_postgres.client import PostgresConnector, PostgresLogBasedStream
from tap_postgres.connection_parameters import ConnectionParameters
from tap_postgres.tap import TapPostgres

DUMMY_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
}


# TODO: should this be a shared fixture/function in conftest?
class DummyConnector(PostgresConnector):
    """Connector that doesn't talk to a real database."""

    def __init__(self, config: dict) -> None:
        params = ConnectionParameters.from_tap_config(config)
        super().__init__(config, params.render_as_sqlalchemy_url())

    def get_table(self, full_table_name, column_names=None):
        return sa.Table("dummy", sa.MetaData(), sa.Column("id", sa.Integer))


@pytest.fixture
def stream() -> PostgresLogBasedStream:
    """A ``PostgresLogBasedStream`` wired against a stub connector."""
    tap = TapPostgres(config=DUMMY_CONFIG, setup_mapper=False)
    catalog_entry = CatalogEntry(
        tap_stream_id="public-users",
        metadata=MetadataMapping.from_iterable(
            [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "inclusion": "available",
                        "selected": True,
                        "schema-name": "public",
                    },
                },
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {"inclusion": "available", "selected": True},
                },
            ]
        ),
        schema=Schema(properties={"id": Schema(type=["integer", "null"])}, type="object"),
        table="users",
    )
    return PostgresLogBasedStream(
        tap,
        catalog_entry.to_dict(),
        connection_parameters=ConnectionParameters.from_tap_config(DUMMY_CONFIG),
        connector=DummyConnector(config=DUMMY_CONFIG),
    )


@pytest.mark.parametrize("action", ["I", "U"], ids=["insert", "update"])
def test_consume_upsert_returns_row_with_sdc_columns(stream, action):
    """Inserts and updates have identical row-construction semantics."""
    payload = {
        "action": action,
        "schema": "public",
        "table": "users",
        "columns": [
            {"name": "id", "type": "integer", "value": 42},
            {"name": "name", "type": "text", "value": "alice"},
        ],
    }
    assert stream.consume(payload, lsn=12345) == {
        "id": 42,
        "name": "alice",
        "_sdc_deleted_at": None,
        "_sdc_lsn": 12345,
    }


def test_consume_delete_uses_identity_and_sets_deleted_at(stream):
    payload = {
        "action": "D",
        "schema": "public",
        "table": "users",
        "identity": [{"name": "id", "type": "integer", "value": 5}],
    }
    row = stream.consume(payload, lsn=55)
    assert row["id"] == 5
    assert row["_sdc_lsn"] == 55
    # a stringly-typed UTC ISO timestamp is set; exact value is time-dependent
    assert isinstance(row["_sdc_deleted_at"], str)
    assert row["_sdc_deleted_at"].endswith("Z")


@pytest.mark.parametrize(
    "action",
    ["T", "B", "C"],
    ids=["truncate", "transaction-begin", "transaction-commit"],
)
def test_consume_non_data_actions_return_none(stream, action):
    """Truncate and transaction begin/commit are non-data and produce no row."""
    assert stream.consume({"action": action}, lsn=1) is None


def test_consume_unknown_action_raises(stream):
    with pytest.raises(RuntimeError, match="unknown action"):
        stream.consume({"action": "X", "columns": []}, lsn=1)


@pytest.mark.parametrize(
    "column_type",
    ["integer", "numeric(10,2)", "bigint", "double precision"],
)
def test_consume_numeric_empty_string_becomes_none(stream, column_type):
    """wal2json sometimes emits ``""`` for numeric columns; treat as NULL."""
    payload = {
        "action": "I",
        "schema": "public",
        "table": "users",
        "columns": [
            {"name": "id", "type": "integer", "value": 1},
            {"name": "amount", "type": column_type, "value": ""},
        ],
    }
    assert stream.consume(payload, lsn=1)["amount"] is None


@pytest.mark.parametrize(
    ["raw", "expected"],
    [
        ('{"action":"B"}', {"action": "B"}),
        (
            '{"action":"I","columns":[{"name":"c","type":""MyEnum"","value":"X"}]}',
            {"action": "I", "columns": [{"name": "c", "type": "MyEnum", "value": "X"}]},
        ),
        ("{not json{", None),
    ],
    ids=["valid", "enum-quote-bug-recovered", "unrecoverable"],
)
def test_parse_wal_message(raw, expected):
    assert parse_wal_message(raw, cursor=None) == expected
