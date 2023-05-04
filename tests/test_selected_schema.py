"""Tests standard tap features using the built-in SDK tests library."""
import json

import pendulum
from singer_sdk.testing.templates import TapTestTemplate

from tap_postgres.tap import TapPostgres

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG = {
    "sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres",
}


def selected_schema_test(tap, table_name):
    """excluding one column from stream and check if it is not present in query"""
    column_to_exclude = "name"
    tap.run_discovery()
    tap_catalog = json.loads(tap.catalog_json_text)
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] != []:
                    if metadata["breadcrumb"][1] == column_to_exclude:
                        metadata["metadata"]["selected"] = False

    tap = TapPostgres(config=SAMPLE_CONFIG, catalog=tap_catalog)
    streams = tap.discover_streams()
    selected_stream = [s for s in streams if s.selected is True][0]

    assert not column_to_exclude in str(selected_stream.get_query())


class TapTestSelectedSchema(TapTestTemplate):
    name = "selected_schema"
    table_name = TABLE_NAME

    def test(self):
        selected_schema_test(self.tap, self.table_name)
