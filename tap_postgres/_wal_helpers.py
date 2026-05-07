"""Helper functions for LOG_BASED replication."""

from __future__ import annotations

import json
import re
import typing as t

import psycopg2

if t.TYPE_CHECKING:
    from psycopg2 import extras


# wal2json emits enum type names with unescaped double quotes, producing invalid JSON
# like "type":""EnumName"" ... strip the extra quotes via regex and re-parse
_WAL2JSON_ENUM_QUOTE_RE = re.compile(r'"type":""([^"]+)""')


def normalize_fqn(schema: str, table: str) -> str:
    """Generate canonical, fully-qualified name for dispatch.

    This is the source of truth for matching a WAL message to a registered stream.
    Both sides -- the tap (when registering streams with the reader) and the WAL reader
    (when dispatching a parsed payload) -- *must* call this function with the raw schema
    and table name strings.

    wal2json's format-version=2 output includes ``"schema"`` and ``"table"`` fields
    as the raw, unquoted identifier names (wal2json reports whatever Postgres has stored).
    Therefore, use the raw names joined by a single dot, with no quoting and no case folding.

    Do *not* use ``SQLStream.fully_qualified_name`` for dispatch.
    """
    return f"{schema}.{table}"


def escape_for_add_tables(identifier: str) -> str:
    """Escape a schema or table name for use in wal2json's ``add-tables``.

    wal2json's ``add-tables`` option takes a comma-separated list of "schema.table" entries.
    Backslash is the escape character; comma and dot within an identifier must be escaped,
    and backslash itself must be doubled.

    References:
        - https://github.com/eulerto/wal2json#parameters
    """
    return identifier.replace("\\", "\\\\").replace(",", "\\,").replace(".", "\\.")


def build_add_tables_option(fqn_pairs: list[tuple[str, str]]) -> str:
    """Build the wal2json ``add-tables`` option from a list of (schema, table).

    Each identifier is escaped with ``escape_for_add_tables`` and joined with
    the appropriate separators. For example::

        >>> build_add_tables_option([("public", "users"), ("public", "orders")])
        'public.users,public.orders'
    """
    return ",".join(
        f"{escape_for_add_tables(schema)}.{escape_for_add_tables(table)}"
        for schema, table in fqn_pairs
    )


def parse_wal_message(raw_payload: str, cursor: extras.ReplicationCursor | None) -> dict | None:
    """Parse a raw wal2json JSON payload into a Python dict.

    Handles the known wal2json enum-quoting bug via one retry after regex repair.
    Returns None if the payload can't be decoded even after repair, in which case
    the caller should log and skip.

    When ``cursor`` is provided, pre-parses ``text[]`` column values into Python lists
    using psycopg2's ``STRINGARRAY`` type caster. This must be done while the cursor is alive,
    since ``STRINGARRAY`` reads connection-level encoding info from it.
    """
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        try:
            payload = json.loads(fix_wal2json_enum_quotes(raw_payload))
        except json.JSONDecodeError:
            return None

    if cursor is not None:
        pre_parse_text_arrays(payload, cursor)

    return payload


def fix_wal2json_enum_quotes(payload: str) -> str:
    """Repair the wal2json enum-quoting bug in a raw JSON payload.

    wal2json outputs enum type names with unescaped double quotes (e.g. "type":""EnumName""),
    which is invalid JSON. Normalize this to "type":"EnumName" so a second ``json.loads``
    attempt succeeds.
    """
    return _WAL2JSON_ENUM_QUOTE_RE.sub(r'"type":"\1"', payload)


def pre_parse_text_arrays(payload: dict, cursor: extras.ReplicationCursor) -> None:
    """Pre-parse ``text[]`` column values in a wal2json payload, in place.

    wal2json returns ``text[]`` values as Postgres's array literal string (e.g. '{a,b,c}').
    Converting to a Python list requires ``psycopg2.extensions.STRINGARRAY``, which needs
    a live cursor for encoding context. Calling this during the WAL read means downstream code
    -- ``consume()``, etc. -- can operate on plain Python lists with no cursor dependency.
    """
    for key in ("columns", "identity"):
        for column in payload.get(key, ()) or ():
            if column.get("type") == "text[]" and column.get("value") is not None:
                column["value"] = psycopg2.extensions.STRINGARRAY(column["value"], cursor)
