import logging
from unittest.mock import patch

import psycopg2
import pytest

from tap_postgres._wal_helpers import (
    ReplicationSlotError,
    build_add_tables_option,
    check_replication_slot,
    escape_for_add_tables,
    normalize_fqn,
)


@pytest.mark.parametrize(
    ["schema", "table", "expected"],
    [
        ("public", "users", "public.users"),
        ("Public", "MyTable", "Public.MyTable"),
        ("weird.schema", "t", "weird.schema.t"),
    ],
    ids=["basic", "preserve-case", "schema-with-dot"],
)
def test_normalize_fqn(schema, table, expected):
    assert normalize_fqn(schema, table) == expected


@pytest.mark.parametrize(
    ["identifier", "expected"],
    [
        ("users", "users"),
        ("a,b", "a\\,b"),
        ("a.b", "a\\.b"),
        ("a\\b", "a\\\\b"),
        ("a\\,b", "a\\\\\\,b"),
    ],
    ids=["basic", "comma", "dot", "backslash", "escaping-order"],
)
def test_escape_for_add_tables(identifier, expected):
    assert escape_for_add_tables(identifier) == expected


@pytest.mark.parametrize(
    ["fqn_pairs", "expected"],
    [
        ([("public", "users")], "public.users"),
        ([("public", "users"), ("app", "orders")], "public.users,app.orders"),
        ([("my,schema", "tbl.name")], "my\\,schema.tbl\\.name"),
    ],
    ids=["single", "multiple", "special-chars"],
)
def test_build_add_tables_option(fqn_pairs, expected):
    assert build_add_tables_option(fqn_pairs) == expected


# replication slot preflight


class _FakeCursor:
    def __init__(self, row: dict | None) -> None:
        self._row = row
        self.description = [(name,) for name in (row or {"slot_name": None})]
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        self.executed.append((query, params))

    def fetchone(self):
        return None if self._row is None else tuple(self._row.values())


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def _slot(**overrides) -> dict:
    """A healthy wal2json slot row, as returned by ``SELECT * FROM pg_replication_slots``."""
    row = {
        "slot_name": "tappostgres",
        "plugin": "wal2json",
        "slot_type": "logical",
        "database": "appdb",
        "active": False,
        "active_pid": None,
        "wal_status": "reserved",
        "conflicting": False,
        "invalidation_reason": None,
    }
    row.update(overrides)
    return row


def _check(row: dict | None):
    cursor = _FakeCursor(row)
    conn = _FakeConnection(cursor)
    with patch("tap_postgres._wal_helpers.psycopg2.connect", return_value=conn):
        check_replication_slot(
            "dbname=appdb", slot_name="tappostgres", database="appdb", logger=_LOGGER
        )
    return cursor, conn


_LOGGER = logging.getLogger("test_wal_helpers")


def test_check_replication_slot_healthy():
    cursor, conn = _check(_slot())
    assert cursor.executed == [
        ("SELECT * FROM pg_replication_slots WHERE slot_name = %s", ("tappostgres",))
    ]
    assert conn.autocommit is True
    assert conn.closed is True


def test_check_replication_slot_pre_pg13_columns():
    """Servers before 13 have no wal_status/conflicting/invalidation_reason columns."""
    row = _slot()
    for key in ("wal_status", "conflicting", "invalidation_reason"):
        del row[key]
    _check(row)


@pytest.mark.parametrize(
    ["row", "match"],
    [
        (None, r"does not exist.*pg_create_logical_replication_slot\('tappostgres', 'wal2json'\)"),
        (_slot(slot_type="physical", plugin=None, database=None), "is a physical slot"),
        (_slot(plugin="pgoutput"), "uses output plugin 'pgoutput'"),
        (_slot(database="otherdb"), "belongs to database 'otherdb', not 'appdb'"),
        (_slot(wal_status="lost"), r"invalidated \(lost\)"),
        (_slot(conflicting=True), r"invalidated \(conflicting\)"),
        (_slot(invalidation_reason="idle_timeout"), r"invalidated \(idle_timeout\)"),
    ],
    ids=["missing", "physical", "wrong-plugin", "wrong-database", "wal-lost", "conflict", "pg17"],
)
def test_check_replication_slot_unusable(row, match):
    with pytest.raises(ReplicationSlotError, match=match):
        _check(row)


@pytest.mark.parametrize(
    ["row", "message"],
    [
        (_slot(active=True, active_pid=4242), "in use by another process (pid 4242)"),
        (_slot(wal_status="unreserved"), "wal_status=unreserved"),
    ],
    ids=["active", "unreserved"],
)
def test_check_replication_slot_warns(row, message, caplog):
    with caplog.at_level(logging.WARNING, logger="test_wal_helpers"):
        _check(row)
    assert message in caplog.text


def test_check_replication_slot_connection_error_is_not_fatal(caplog):
    """If the check can't run, keep today's behavior and let the sync proceed."""
    with (
        patch(
            "tap_postgres._wal_helpers.psycopg2.connect",
            side_effect=psycopg2.OperationalError("could not connect"),
        ),
        caplog.at_level(logging.WARNING, logger="test_wal_helpers"),
    ):
        check_replication_slot(
            "dbname=appdb", slot_name="tappostgres", database="appdb", logger=_LOGGER
        )
    assert "Could not check replication slot 'tappostgres'" in caplog.text
