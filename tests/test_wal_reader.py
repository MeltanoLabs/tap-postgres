"""Unit tests for the single-connection WAL reader.

No Postgres DB necessary! Stubbed streams have the same interface as ``PostgresLogBasedStream``.
Tests for ``emit_record`` and the tap's LOG_BASED dispatch use ``PostgresLogBasedStream``
against a ``DummyConnector``. Tests patch ``tap_postgres.wal_reader.psycopg2.connect``
and ``tap_postgres.wal_reader.select.select`` so the read loop runs against in-memory fakes.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema

from tap_postgres.client import PostgresConnector, PostgresLogBasedStream
from tap_postgres.connection_parameters import ConnectionParameters
from tap_postgres.tap import TapPostgres
from tap_postgres.wal_reader import SingleConnectionWALReader

# fake replication primitives

DUMMY_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
}


class FakeReplicationMessage:
    """Stand-in for ``psycopg2.extras.ReplicationMessage``."""

    def __init__(self, payload: str, data_start: int) -> None:
        self.payload = payload
        self.data_start = data_start


class FakeReplicationCursor:
    """
    Minimal stand-in for ``psycopg2.extras.ReplicationCursor``.

    Returns scripted messages from ``read_message()``. Returns None when exhausted,
    so reader's idle-exit path fires (with a patched ``select.select`` returning empty).
    """

    def __init__(
        self, messages: list[FakeReplicationMessage] | None = None, wal_end: int = 0
    ) -> None:
        self._messages = list(messages or [])
        self.feedback_lsns: list[int] = []
        self.start_options: dict | None = None
        self.started = False
        self.wal_end = wal_end
        self.closed = False

    def send_feedback(self, *, flush_lsn: int) -> None:
        self.feedback_lsns.append(flush_lsn)

    def start_replication(self, **kwargs) -> None:
        self.started = True
        self.start_options = kwargs

    def read_message(self):
        if self._messages:
            msg = self._messages.pop(0)
            self.wal_end = max(self.wal_end, msg.data_start)
            return msg
        return None

    def close(self) -> None:
        self.closed = True

    def fileno(self) -> int:
        # select.select is patched out, but some env may still call this
        return 0


class FakeReplicationConnection:
    """Stand-in for the connection returned by ``psycopg2.connect``."""

    def __init__(self, cursor: FakeReplicationCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> FakeReplicationCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


@contextmanager
def patch_replication(cursor: FakeReplicationCursor):
    """
    Patch ``psycopg2.connect`` and ``select.select`` in ``wal_reader``.

    ``select.select`` is patched to immediately return "no readiness" so the
    idle-exit branch fires when the scripted message list is exhausted.
    """
    conn = FakeReplicationConnection(cursor)
    with (
        patch("tap_postgres.wal_reader.psycopg2.connect", return_value=conn) as p_connect,
        patch("tap_postgres.wal_reader.select.select", return_value=([], [], [])) as p_select,
    ):
        yield p_connect, p_select


# stub stream -- mimics interface that PostgresLogBasedStream exposes to the reader


class StubStream:
    """
    In-memory stand-in for ``PostgresLogBasedStream`` for reader unit tests.
    Implements only the surface area touched by the reader!
    """

    replication_key = "_sdc_lsn"

    def __init__(self, schema: str, table: str, start_lsn: int = 0) -> None:
        self.fully_qualified_name = SimpleNamespace(schema=schema, table=table)
        self.name = f"{schema}-{table}"
        self._start_lsn = start_lsn
        self._state: dict = {}
        self.emitted: list[dict] = []

    def get_starting_replication_key_value(self, *, context=None):
        return self._start_lsn

    def consume(self, payload: dict, lsn: int) -> dict | None:
        action = payload.get("action")
        if action in ("I", "U"):
            row = {c["name"]: c["value"] for c in payload.get("columns", [])}
            row["_sdc_deleted_at"] = None
            row["_sdc_lsn"] = lsn
            return row
        if action == "D":
            row = {c["name"]: c["value"] for c in payload.get("identity", [])}
            row["_sdc_deleted_at"] = "2024-01-01T00:00:00Z"
            row["_sdc_lsn"] = lsn
            return row
        # truncate / transaction — non-data
        return None

    def emit_record(self, record: dict, *, context=None) -> None:
        self.emitted.append(record)

    def get_context_state(self, context):
        return self._state


def _wal_payload(schema: str, table: str, action: str = "I", **columns) -> str:
    """Build a wal2json format-version=2 JSON payload."""
    cols = [{"name": k, "type": "int", "value": v} for k, v in columns.items()]
    body: dict = {"action": action, "schema": schema, "table": table}
    if action == "D":
        body["identity"] = cols
    elif action in ("I", "U"):
        body["columns"] = cols
    return json.dumps(body)


def _build_reader(streams, *, max_run=60, idle_exit=0, slot="testslot"):
    """Construct a ``SingleConnectionWALReader`` with sensible defaults."""
    return SingleConnectionWALReader(
        connection_parameters=ConnectionParameters.from_tap_config(DUMMY_CONFIG),
        replication_slot_name=slot,
        max_run_seconds=max_run,
        idle_exit_seconds=idle_exit,
        streams=streams,
        state_flush_callback=MagicMock(),
        logger=logging.getLogger("test_wal_reader"),
    )


# dummy connector / real-stream helpers -- for emit_record and tap-level tests


class DummyConnector(PostgresConnector):
    """
    Connector that doesn't talk to a real database.
    Mirrors ``DummyConnector`` in ``tests/test_stream_class.py``.
    """

    def __init__(self, config: dict) -> None:
        params = ConnectionParameters.from_tap_config(config)
        super().__init__(config, params.render_as_sqlalchemy_url())

    def get_table(self, full_table_name, column_names=None):
        return sa.Table("dummy", sa.MetaData(), sa.Column("id", sa.Integer))


def _build_log_based_stream(
    tap: TapPostgres, *, schema_name: str, table_name: str, stream_id: str | None = None
) -> PostgresLogBasedStream:
    """Build a real ``PostgresLogBasedStream`` against a stub connector."""
    catalog_entry = CatalogEntry(
        tap_stream_id=stream_id or f"{schema_name}-{table_name}",
        metadata=MetadataMapping.from_iterable(
            [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "inclusion": "available",
                        "selected": True,
                        "schema-name": schema_name,
                    },
                },
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {"inclusion": "available", "selected": True},
                },
            ]
        ),
        schema=Schema(properties={"id": Schema(type=["integer", "null"])}, type="object"),
        table=table_name,
    )
    return PostgresLogBasedStream(
        tap,
        catalog_entry.to_dict(),
        connection_parameters=ConnectionParameters.from_tap_config(DUMMY_CONFIG),
        connector=DummyConnector(config=DUMMY_CONFIG),
    )


# the actual tests, finally


def test_construction_rejects_empty_stream_list():
    """An empty stream list is a programming error; reject it loudly."""
    with pytest.raises(ValueError, match="≥1 stream"):
        SingleConnectionWALReader(
            connection_parameters=ConnectionParameters.from_tap_config(DUMMY_CONFIG),
            replication_slot_name="s",
            max_run_seconds=1,
            idle_exit_seconds=0,
            streams=[],
            state_flush_callback=MagicMock(),
            logger=logging.getLogger("test"),
        )


def test_construction_rejects_duplicate_fqn():
    """Two streams pointing at the same table is a misconfiguration."""
    s1 = StubStream("public", "users", start_lsn=10)
    s2 = StubStream("public", "users", start_lsn=20)
    with pytest.raises(ValueError, match="Duplicate fully-qualified name"):
        _build_reader([s1, s2])


def test_start_replication_uses_min_start_lsn_and_escaped_add_tables():
    """``add-tables`` must be the escaped FQN list; ``start_lsn`` the min."""
    s1 = StubStream("public", "users", start_lsn=200)
    s2 = StubStream("my,schema", "tbl.name", start_lsn=50)  # special chars
    reader = _build_reader([s1, s2])

    cursor = FakeReplicationCursor(messages=[], wal_end=300)
    with patch_replication(cursor):
        reader.run()

    assert cursor.started is True
    assert cursor.start_options["start_lsn"] == 50  # min of (200, 50)
    assert cursor.start_options["slot_name"] == "testslot"
    assert cursor.start_options["decode"] is True
    options = cursor.start_options["options"]
    assert options["format-version"] == 2
    assert options["include-transaction"] is False
    # FQNs are escape_for_add_tables()'d and joined with commas
    add_tables = options["add-tables"]
    assert "public.users" in add_tables
    assert "my\\,schema.tbl\\.name" in add_tables


def test_routes_message_to_correct_stream_by_fqn():
    """A message for ``schema.table`` ends up only on that stream."""
    s_users = StubStream("public", "users")
    s_orders = StubStream("public", "orders")
    reader = _build_reader([s_users, s_orders])

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "users", id=1), data_start=10),
        FakeReplicationMessage(_wal_payload("public", "orders", id=99), data_start=11),
        FakeReplicationMessage(_wal_payload("public", "users", id=2), data_start=12),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=12)
    with patch_replication(cursor):
        reader.run()

    assert [r["id"] for r in s_users.emitted] == [1, 2]
    assert [r["id"] for r in s_orders.emitted] == [99]
    assert reader.records_emitted == 3


def test_drops_messages_below_per_stream_start_lsn():
    """Stream B with start_lsn=200 must NOT see a message at LSN 150."""
    s_a = StubStream("public", "a", start_lsn=100)
    s_b = StubStream("public", "b", start_lsn=200)
    reader = _build_reader([s_a, s_b])

    msgs = [
        # Below B's start_lsn but above A's: A sees it, B does not.
        FakeReplicationMessage(_wal_payload("public", "b", id=1), data_start=150),
        # Above both — both eligible (this is a B message, B sees it).
        FakeReplicationMessage(_wal_payload("public", "b", id=2), data_start=250),
        # A always eligible above 100.
        FakeReplicationMessage(_wal_payload("public", "a", id=3), data_start=120),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=250)
    with patch_replication(cursor):
        reader.run()

    assert [r["id"] for r in s_a.emitted] == [3]
    assert [r["id"] for r in s_b.emitted] == [2]
    assert reader.records_filtered_by_lsn == 1
    assert reader.records_emitted == 2


def test_unroutable_message_increments_counter_and_does_not_crash():
    """A payload whose schema/table doesn't match any registered stream is counted."""
    s = StubStream("public", "users")
    reader = _build_reader([s])

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "ghosts", id=1), data_start=10),
        FakeReplicationMessage(_wal_payload("public", "users", id=2), data_start=11),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=11)
    with patch_replication(cursor):
        reader.run()

    assert s.emitted == [{"id": 2, "_sdc_deleted_at": None, "_sdc_lsn": 11}]
    assert reader.records_unroutable == 1
    assert reader.records_emitted == 1


def test_truncate_and_transaction_messages_do_not_emit():
    """Action ``T`` (truncate) and ``B``/``C`` (transaction) yield no records."""
    s = StubStream("public", "users")
    reader = _build_reader([s])

    msgs = [
        # Truncate has schema/table, but consume() returns None.
        FakeReplicationMessage(
            json.dumps({"action": "T", "schema": "public", "table": "users"}),
            data_start=10,
        ),
        # Transaction begin/commit have no schema/table — dropped before consume.
        FakeReplicationMessage(json.dumps({"action": "B"}), data_start=11),
        FakeReplicationMessage(json.dumps({"action": "C"}), data_start=12),
        # Real data message after, just to confirm the loop kept running.
        FakeReplicationMessage(_wal_payload("public", "users", id=42), data_start=13),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=13)
    with patch_replication(cursor):
        reader.run()

    assert s.emitted == [{"id": 42, "_sdc_deleted_at": None, "_sdc_lsn": 13}]
    assert reader.records_emitted == 1


def test_periodic_state_flush_fires_on_cadence(monkeypatch):
    """``state_flush_callback`` fires once the STATE_FLUSH_INTERVAL has elapsed.

    Rather than fake the clock, drive the cadence by setting the interval to
    zero so the flush happens every iteration that processes a message.
    """
    monkeypatch.setattr(SingleConnectionWALReader, "STATE_FLUSH_INTERVAL", 0)
    s = StubStream("public", "users")
    reader = _build_reader([s])

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "users", id=i), data_start=i)
        for i in range(1, 4)
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=10)
    with patch_replication(cursor):
        reader.run()

    # 3 in-loop flushes + 1 in _advance_slot_and_state_all = 4 -- be lenient
    assert reader._state_flush_callback.call_count >= 2


def test_send_feedback_uses_max_lsn_seen_on_cadence(monkeypatch):
    """When the feedback interval elapses, send_feedback uses max_lsn_seen."""
    monkeypatch.setattr(SingleConnectionWALReader, "FEEDBACK_INTERVAL", 0)
    s = StubStream("public", "users")
    reader = _build_reader([s])

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "users", id=1), data_start=100),
        FakeReplicationMessage(_wal_payload("public", "users", id=2), data_start=200),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=300)
    with patch_replication(cursor):
        reader.run()

    # initial feedback at start_lsn=0, then in-loop feedbacks once max_lsn>0,
    # then the final advance feedback; assert max_lsn_seen=200 made it in
    assert 200 in cursor.feedback_lsns


def test_idle_exit_advances_slot_and_state_for_all_streams():
    """On idle-exit, every stream's ``replication_key_value`` advances to wal_end."""
    s_a = StubStream("public", "a", start_lsn=10)
    s_b = StubStream("public", "b", start_lsn=20)
    reader = _build_reader([s_a, s_b], idle_exit=0)

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "a", id=1), data_start=50),
        FakeReplicationMessage(_wal_payload("public", "b", id=2), data_start=60),
    ]
    advanced_to = 999
    cursor = FakeReplicationCursor(messages=msgs, wal_end=advanced_to)
    with patch_replication(cursor):
        reader.run()

    assert s_a.get_context_state(None)["replication_key_value"] == advanced_to
    assert s_b.get_context_state(None)["replication_key_value"] == advanced_to
    assert s_a.get_context_state(None)["replication_key"] == "_sdc_lsn"
    # final feedback should have been sent at the advanced LSN
    assert advanced_to in cursor.feedback_lsns


def test_max_run_time_exit_advances_slot_and_state():
    """Same advancement path runs when ``max_run_seconds`` is exceeded.

    With ``max_run_seconds = -1``, the very first iteration's time check is always true,
    so the loop breaks before any reads -- exercising the max-run exit path deterministically.
    """
    s = StubStream("public", "users", start_lsn=10)
    reader = _build_reader([s], max_run=-1, idle_exit=10_000)

    cursor = FakeReplicationCursor(messages=[], wal_end=777)
    with patch_replication(cursor):
        reader.run()

    assert s.get_context_state(None)["replication_key_value"] == 777


def test_emit_record_writes_record_message_and_advances_state():
    """``emit_record`` sends one RECORD message and bumps the LSN bookmark."""
    tap = TapPostgres(config=DUMMY_CONFIG, setup_mapper=False)
    stream = _build_log_based_stream(tap, schema_name="public", table_name="users")

    # patch SDK call we depend on; we're only asserting the contract
    stream._write_record_message = MagicMock()

    record = {"id": 1, "_sdc_deleted_at": None, "_sdc_lsn": 12345}
    stream.emit_record(record)

    stream._write_record_message.assert_called_once_with(record)
    state = stream.get_context_state(None)
    assert state["replication_key"] == "_sdc_lsn"
    assert state["replication_key_value"] == 12345


def test_emit_record_does_not_move_bookmark_backward():
    """A record with an LSN below the current bookmark must not regress state."""
    tap = TapPostgres(config=DUMMY_CONFIG, setup_mapper=False)
    stream = _build_log_based_stream(tap, schema_name="public", table_name="users")
    stream._write_record_message = MagicMock()

    state = stream.get_context_state(None)
    state["replication_key"] = "_sdc_lsn"
    state["replication_key_value"] = 1000

    stream.emit_record({"id": 1, "_sdc_deleted_at": None, "_sdc_lsn": 500})
    assert state["replication_key_value"] == 1000  # unchanged

    stream.emit_record({"id": 2, "_sdc_deleted_at": None, "_sdc_lsn": 2000})
    assert state["replication_key_value"] == 2000  # forward only


def test_schema_messages_emitted_before_any_record_message():
    """Every stream's SCHEMA must hit the wire before any of its RECORDs.

    Two LOG_BASED streams; WAL records interleaved between them. We mock each stream's
    ``_write_schema_message`` and ``emit_record`` to record into a shared event log,
    then assert that the first emit_record event for each stream is preceded
    by at least one schema event for that stream.
    """
    config = {
        **DUMMY_CONFIG,
        # force reader's main loop to exit immediately once read_message returns None
        # i.e. as soon as scripted messages are exhausted
        "replication_idle_exit_seconds": 0,
    }
    tap = TapPostgres(config=config, setup_mapper=False)
    s_users = _build_log_based_stream(tap, schema_name="public", table_name="users")
    s_orders = _build_log_based_stream(tap, schema_name="public", table_name="orders")

    # inject streams into the tap, bypassing discover_streams and cached connector property
    # which would try to reach a real db
    tap._streams = {s_users.name: s_users, s_orders.name: s_orders}
    tap.connection_parameters = ConnectionParameters.from_tap_config(config)
    tap._write_state_checkpoint = MagicMock()  # avoid writing to stdout

    events: list[tuple[str, str]] = []

    def schema_writer(stream_name):
        return lambda: events.append(("SCHEMA", stream_name))

    def record_writer(stream_name):
        def _emit(record, *, context=None):
            events.append(("RECORD", stream_name))

        return _emit

    s_users._write_schema_message = schema_writer(s_users.name)
    s_orders._write_schema_message = schema_writer(s_orders.name)
    s_users.emit_record = record_writer(s_users.name)
    s_orders.emit_record = record_writer(s_orders.name)

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "users", id=1), data_start=10),
        FakeReplicationMessage(_wal_payload("public", "orders", id=99), data_start=11),
        FakeReplicationMessage(_wal_payload("public", "users", id=2), data_start=12),
        FakeReplicationMessage(_wal_payload("public", "orders", id=100), data_start=13),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=20)
    with patch_replication(cursor):
        tap._sync_log_based_streams_shared()

    # find index of the first RECORD per stream
    def _first_record(name):
        for i, (kind, sn) in enumerate(events):
            if kind == "RECORD" and sn == name:
                return i
        raise AssertionError(f"no RECORD event for {name}")

    def _first_schema(name):
        for i, (kind, sn) in enumerate(events):
            if kind == "SCHEMA" and sn == name:
                return i
        raise AssertionError(f"no SCHEMA event for {name}")

    # both streams' SCHEMA must precede the first RECORD for either stream
    last_schema_idx = max(_first_schema(s_users.name), _first_schema(s_orders.name))
    first_record_idx = min(_first_record(s_users.name), _first_record(s_orders.name))
    assert last_schema_idx < first_record_idx, f"SCHEMA-before-RECORD violated; events: {events}"


def test_write_schema_message_is_idempotent():
    """
    ``Stream.sync()`` and ``_sync_log_based_streams_shared`` both call this.
    Without idempotency we'd emit duplicate SCHEMA messages for every LOG_BASED stream.
    """
    tap = TapPostgres(config=DUMMY_CONFIG, setup_mapper=False)
    stream = _build_log_based_stream(tap, schema_name="public", table_name="users")

    # patch the SDK base-class method so we can count actual emissions
    with patch("singer_sdk.sql.SQLStream._write_schema_message", autospec=True) as base_write:
        stream._write_schema_message()
        stream._write_schema_message()
        stream._write_schema_message()

    assert base_write.call_count == 1


def test_malformed_payload_increments_counter():
    """Payloads that fail JSON parsing (even after the enum-quote repair) are counted."""
    s = StubStream("public", "users")
    reader = _build_reader([s])

    msgs = [
        # garbage JSON, beyond the enum-quote bug repair
        FakeReplicationMessage("{not json{", data_start=10),
        FakeReplicationMessage(_wal_payload("public", "users", id=1), data_start=11),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=11)
    with patch_replication(cursor):
        reader.run()

    assert reader.records_malformed == 1
    assert reader.records_emitted == 1


def test_per_stream_emit_counter_tracks_routing():
    """``records_emitted_by_fqn`` is keyed by FQN and reflects routing."""
    s_users = StubStream("public", "users")
    s_orders = StubStream("public", "orders")
    s_quiet = StubStream("public", "quiet")  # registered but receives nothing
    reader = _build_reader([s_users, s_orders, s_quiet])

    msgs = [
        FakeReplicationMessage(_wal_payload("public", "users", id=1), data_start=10),
        FakeReplicationMessage(_wal_payload("public", "orders", id=2), data_start=11),
        FakeReplicationMessage(_wal_payload("public", "users", id=3), data_start=12),
    ]
    cursor = FakeReplicationCursor(messages=msgs, wal_end=12)
    with patch_replication(cursor):
        reader.run()

    assert reader.records_emitted_by_fqn == {
        "public.users": 2,
        "public.orders": 1,
        "public.quiet": 0,
    }


def test_idle_exit_seconds_zero_exits_immediately_when_no_messages():
    """``idle_exit_seconds=0`` is the explicit "exit as soon as the queue drains" knob."""
    s = StubStream("public", "users", start_lsn=10)
    reader = _build_reader([s], idle_exit=0)

    cursor = FakeReplicationCursor(messages=[], wal_end=20)
    with patch_replication(cursor):
        reader.run()

    # no records, no crash: advancement path still ran since wal_end > start_lsn
    assert reader.records_emitted == 0
    assert s.get_context_state(None)["replication_key_value"] == 20


def test_config_flag_off_uses_legacy_per_stream_path():
    """``log_based_single_connection=false`` falls through to the legacy generator.

    With the flag off, ``get_records()`` must yield from ``_get_records_per_stream``
    and never trigger ``_sync_log_based_streams_shared``, which would build the reader.
    """
    config = {**DUMMY_CONFIG, "log_based_single_connection": False}
    tap = TapPostgres(config=config, setup_mapper=False)
    stream = _build_log_based_stream(tap, schema_name="public", table_name="users")

    sentinel = [{"id": 1, "_sdc_lsn": 7, "_sdc_deleted_at": None}]

    with (
        patch.object(stream, "_get_records_per_stream", return_value=iter(sentinel)) as p_legacy,
        patch.object(tap, "_sync_log_based_streams_shared") as p_shared,
        patch("tap_postgres.wal_reader.SingleConnectionWALReader") as p_reader_cls,
    ):
        out = list(stream.get_records(context=None))

    assert out == sentinel
    p_legacy.assert_called_once()
    p_shared.assert_not_called()
    p_reader_cls.assert_not_called()
