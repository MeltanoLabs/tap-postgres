import pytest

from tap_postgres._wal_helpers import build_add_tables_option, escape_for_add_tables, normalize_fqn


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
