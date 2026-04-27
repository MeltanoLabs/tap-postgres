"""Helper functions for LOG_BASED replication."""


def normalize_fqn(schema: str, table: str) -> str:
    """Generate canonical, fully-qualified name for dispatch.

    This is the source of truth for matching a WAL message to a registered stream.
    Both sides — the tap (when registering streams with the reader) and the WAL reader
    (when dispatching a parsed payload) — MUST call this function with the raw schema
    and table name strings.

    wal2json's format-version=2 output includes ``"schema"`` and ``"table"``
    fields as the raw, unquoted identifier names (Postgres stores identifiers
    case-folded to lowercase unless they were originally quoted at DDL time;
    wal2json reports whatever Postgres has stored). We therefore use the raw
    names joined by a single dot, with no quoting and no case folding.

    Do NOT use ``SQLStream.fully_qualified_name`` for dispatch: some SDK
    versions quote identifiers (e.g. ``"public"."MyTable"``) or use a
    hyphen-separated ``tap_stream_id`` form, neither of which matches
    wal2json output.
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
    # backslash first, then separators
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
