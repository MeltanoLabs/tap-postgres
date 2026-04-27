"""Single-connection WAL reader for LOG_BASED streams.

One ``SingleConnectionWALReader`` replaces N per-stream replication connections.
It opens one ``LogicalReplicationConnection``, starts replication with ``add-tables``
listing every LOG_BASED stream's table, and dispatches each incoming wal2json message
to the owning stream's ``emit_record()`` method for immediate emission as a Singer RECORD.
"""

import datetime
import logging
import select
import typing as t
from collections.abc import Callable

import psycopg2
from psycopg2 import extras

from tap_postgres._wal_helpers import build_add_tables_option, normalize_fqn, parse_wal_message

if t.TYPE_CHECKING:
    from tap_postgres.client import PostgresLogBasedStream
    from tap_postgres.connection_parameters import ConnectionParameters


class SingleConnectionWALReader:
    """Reads the WAL once and emits records from all LOG_BASED streams inline.

    Initialize one instance, and call ``run()`, once per tap run. Responsible for:

    - opening one ``LogicalReplicationConnection``
    - starting replication with ``start_lsn = min(bookmark for all streams)``
      and ``add-tables`` listing every registered table
    - reading wal2json messages in a loop, bounded by ``replication_max_run_seconds``
      and exiting early after ``replication_idle_exit_seconds`` of no data
    - parsing each message, dispatching to the correct stream via ``normalize_fqn()``,
      and calling ``stream.emit_record(row)`` for records that pass the per-stream
      LSN filter
    - emitting STATE messages on a 30s cadence via ``state_flush_callback``
    - sending replication-slot feedback on a 30s cadence, using the maximum LSN
      seen so far as the flush point
    - advancing the slot and per-stream bookmarks to the server's current WAL tip
      on exit (same as ``_advance_slot_and_state``, but for all streams at once)

    Per-stream LSN filtering: when ``min(start_lsn)`` is used to open the connection,
    some messages will have LSN below some streams' individual bookmarks. Those messages
    are already-processed for those streams and must be dropped, *not* re-emitted.
    Each stream's own ``start_lsn`` is used to filter dispatches to it.
    """

    STATUS_INTERVAL = 10  # seconds between server keep-alives
    SELECT_TIMEOUT = 1.0  # seconds to block in select() when no message
    FEEDBACK_INTERVAL = 30  # seconds between send_feedback calls
    STATE_FLUSH_INTERVAL = 30  # seconds between STATE message emissions

    def __init__(
        self,
        *,
        connection_parameters: "ConnectionParameters",
        replication_slot_name: str,
        max_run_seconds: int,
        idle_exit_seconds: int,
        streams: list["PostgresLogBasedStream"],
        state_flush_callback: Callable[[], None],
        logger: logging.Logger,
    ) -> None:
        """Initialize WAL reader.

        Args:
            connection_parameters: Database connection parameters (shared with the tap)
            replication_slot_name: Name of the wal2json replication slot
            max_run_seconds: Hard upper bound on run duration
            idle_exit_seconds: Exit if no data messages arrive for this long
            streams: All selected LOG_BASED streams. Must be non-empty.
            state_flush_callback: Called every ``STATE_FLUSH_INTERVAL`` seconds to emit
                a Singer STATE message. The callback reads the tap's current state dict
                and writes the message.
            logger: Logger for progress and slot-advancement messages.
        """
        if not streams:
            raise ValueError("SingleConnectionWALReader requires ≥1 stream")

        self._connection_parameters = connection_parameters
        self._replication_slot_name = replication_slot_name
        self._max_run_seconds = max_run_seconds
        self._idle_exit_seconds = idle_exit_seconds
        self._state_flush_callback = state_flush_callback
        self._logger = logger

        # dispatch mapping: normalized FQN => (stream, start_lsn)
        # start_lsn is captured at construction so filtering is cheap
        # and doesn't re-read state mid-run
        self._streams_by_fqn: dict[str, tuple[PostgresLogBasedStream, int]] = {}
        for stream in streams:
            fqn_obj = stream.fully_qualified_name
            if fqn_obj.schema is None:
                raise ValueError(
                    f"Stream {stream.name!r} has no schema in its fully-qualified name; "
                    f"cannot register with the single-connection WAL reader"
                )
            fqn = normalize_fqn(fqn_obj.schema, fqn_obj.table)
            bookmark = stream.get_starting_replication_key_value(context=None)
            start_lsn = bookmark if bookmark is not None else 0
            if fqn in self._streams_by_fqn:
                raise ValueError(
                    f"Duplicate fully-qualified name {fqn!r} among LOG_BASED "
                    f"streams; each table may be selected only once."
                )
            self._streams_by_fqn[fqn] = (stream, start_lsn)

        self.records_emitted = 0
        self.records_filtered_by_lsn = 0
        self.records_unroutable = 0
        self.records_malformed = 0
        # per-FQN counters, useful for "is stream X actually getting any data?" debugging
        # initialized for every registered stream so the dict is complete even if zero records
        self.records_emitted_by_fqn: dict[str, int] = dict.fromkeys(self._streams_by_fqn, 0)

    def run(self) -> None:
        """Execute single-connection WAL read loop.

        This is synchronous and blocks until either: no data message has arrived
        for ``idle_exit_seconds`` OR ``max_run_seconds`` has elapsed. On exit, advances
        replication slot to the current WAL tip and updates streams' bookmarks to that tip.
        """
        global_start_lsn = min(start_lsn for _, start_lsn in self._streams_by_fqn.values())
        fqn_objs = [stream.fully_qualified_name for stream, _ in self._streams_by_fqn.values()]
        add_tables = build_add_tables_option(
            [(fqn_obj.schema, fqn_obj.table) for fqn_obj in fqn_objs]
        )
        self._logger.info(
            "Starting single-connection WAL read for %d stream(s) from LSN %d",
            len(self._streams_by_fqn),
            global_start_lsn,
        )

        conn = psycopg2.connect(
            self._connection_parameters.render_as_psycopg2_dsn(),
            connection_factory=extras.LogicalReplicationConnection,
        )
        cursor = conn.cursor()
        try:
            cursor.send_feedback(flush_lsn=global_start_lsn)
            cursor.start_replication(
                slot_name=self._replication_slot_name,
                decode=True,
                start_lsn=global_start_lsn,
                status_interval=self.STATUS_INTERVAL,
                options={
                    "format-version": 2,
                    "include-transaction": False,
                    "add-tables": add_tables,
                },
            )
            self._run_loop(cursor)
            self._advance_slot_and_state_all(cursor, global_start_lsn)
        finally:
            cursor.close()
            conn.close()

        self._logger.info(
            "WAL read complete: %d records emitted, %d filtered by per-stream LSN, "
            "%d unroutable, %d malformed",
            self.records_emitted,
            self.records_filtered_by_lsn,
            self.records_unroutable,
            self.records_malformed,
        )
        self._logger.info(
            "Per-stream record counts: %s",
            {fqn: self.records_emitted_by_fqn[fqn] for fqn in self._streams_by_fqn},
        )

    def _run_loop(self, cursor: extras.ReplicationCursor) -> None:
        """Inner read / dispatch / periodic-flush loop."""
        run_start = datetime.datetime.now()
        last_data_message = run_start
        last_feedback = run_start
        last_state_flush = run_start
        max_lsn_seen = 0

        while True:
            now = datetime.datetime.now()
            # total time budget check
            if (now - run_start).total_seconds() > self._max_run_seconds:
                self._logger.info(
                    "Reached max run time of %d seconds (%d records emitted)",
                    self._max_run_seconds,
                    self.records_emitted,
                )
                break

            # periodic STATE emission
            if (now - last_state_flush).total_seconds() >= self.STATE_FLUSH_INTERVAL:
                self._state_flush_callback()
                last_state_flush = now

            # periodic replication-slot feedback
            if max_lsn_seen > 0 and (now - last_feedback).total_seconds() >= self.FEEDBACK_INTERVAL:
                try:
                    cursor.send_feedback(flush_lsn=max_lsn_seen)
                    last_feedback = now
                except Exception as exc:
                    self._logger.warning("send_feedback failed: %s", exc)

            # read the next WAL message
            message = cursor.read_message()
            if message is not None:
                last_data_message = datetime.datetime.now()
                self._dispatch(cursor, message)
                max_lsn_seen = max(max_lsn_seen, message.data_start)
                continue

            # no message available -- block briefly and check idle exit
            try:
                ready = select.select([cursor], [], [], self.SELECT_TIMEOUT)[0]
            except InterruptedError:
                ready = [cursor]

            if not ready:
                data_idle = (datetime.datetime.now() - last_data_message).total_seconds()
                if data_idle >= self._idle_exit_seconds:
                    self._logger.info(
                        "No data for %.0f s, ending WAL read (%d records emitted in %.0f s)",
                        data_idle,
                        self.records_emitted,
                        (datetime.datetime.now() - run_start).total_seconds(),
                    )
                    break

    def _dispatch(
        self, cursor: extras.ReplicationCursor, message: extras.ReplicationMessage
    ) -> None:
        """Parse one WAL message and hand it to the owning stream."""
        # parse (+pre-parse text[] values) while the cursor is alive
        payload = parse_wal_message(message.payload, cursor)
        if payload is None:
            self._logger.warning(
                "A message payload of %s could not be converted to JSON",
                message.payload,
            )
            self.records_malformed += 1
            return

        # non-data messages (transactions, truncates) have no schema/table
        # consume() returns None for them and we're done
        schema_name = payload.get("schema")
        table_name = payload.get("table")
        if schema_name is None or table_name is None:
            return

        fqn = normalize_fqn(schema_name, table_name)
        routed = self._streams_by_fqn.get(fqn)
        if routed is None:
            # this should never happen: add-tables filters at the server, so we only receive
            # messages for registered tables... count it and move on; a non-zero counter
            # in logs is a signal to investigate (e.g. a normalize_fqn format mismatch)
            self.records_unroutable += 1
            self._logger.debug("Received message for unregistered table %s; dropping", fqn)
            return

        stream, stream_start_lsn = routed

        # per-stream LSN filter: because start_replication was opened at min(start_lsn),
        # streams with higher bookmarks will see some already-processed messages
        # it's safe to drop them silently
        if message.data_start < stream_start_lsn:
            self.records_filtered_by_lsn += 1
            return

        row = stream.consume(payload, message.data_start)
        if not row:
            return

        stream.emit_record(row)
        self.records_emitted += 1
        self.records_emitted_by_fqn[fqn] += 1

    def _advance_slot_and_state_all(self, cursor: extras.ReplicationCursor, start_lsn: int) -> None:
        """Advance slot to the WAL tip and update every stream's bookmark.

        Mirrors ``PostgresLogBasedStream._advance_slot_and_state`` but applies resulting LSN
        to every registered stream.
        """
        # prefer server-reported wal_end if it's ahead of start_lsn, otherwise query the server
        flush_lsn: int | None = None
        try:
            wal_end = getattr(cursor, "wal_end", None)
            if wal_end is not None and wal_end > start_lsn:
                flush_lsn = wal_end
        except Exception:
            pass

        if flush_lsn is None or flush_lsn <= start_lsn:
            flush_lsn = self._query_current_wal_lsn()

        if flush_lsn is None or flush_lsn <= start_lsn:
            return

        try:
            cursor.send_feedback(flush_lsn=flush_lsn)
        except Exception as exc:
            self._logger.warning("Final send_feedback failed: %s", exc)
            return

        self._logger.info(
            "Advanced replication slot from %d to %d (delta %.2f MB)",
            start_lsn,
            flush_lsn,
            (flush_lsn - start_lsn) / (1024 * 1024),
        )

        # update every stream's bookmark to the advanced LSN
        # for streams whose per-stream start_lsn was already above this value, skip --
        # don't move bookmarks backward!
        for stream, stream_start_lsn in self._streams_by_fqn.values():
            if flush_lsn <= stream_start_lsn:
                continue
            state = stream.get_context_state(context=None)
            state["replication_key"] = stream.replication_key
            state["replication_key_value"] = flush_lsn

        # one final STATE emission so the next run picks up the advance
        self._state_flush_callback()

    def _query_current_wal_lsn(self) -> int | None:
        """Query ``pg_current_wal_flush_lsn()`` on a non-replication conn."""
        try:
            conn = psycopg2.connect(
                self._connection_parameters.render_as_psycopg2_dsn(),
            )
            try:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_current_wal_flush_lsn()")
                    row = cur.fetchone()
                    if row is None:
                        return None
                    hi, lo = row[0].split("/")
                    return (int(hi, 16) << 32) + int(lo, 16)
            finally:
                conn.close()
        except Exception as exc:
            self._logger.warning("Could not query current WAL LSN: %s", exc)
            return None
