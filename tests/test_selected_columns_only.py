"""Tests selected columns only from stream """
import json

from singer_sdk.testing.templates import TapTestTemplate

from tap_postgres.tap import TapPostgres
from tests.settings import DB_SQLALCHEMY_URL

TABLE_NAME_SELECTED_COLUMNS_ONLY = "test_selected_columns_only"
SAMPLE_CONFIG = {
    "sqlalchemy_url": DB_SQLALCHEMY_URL,
}


def selected_columns_only_test(tap, table_name):
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
                if (
                    metadata["breadcrumb"] != []
                    and metadata["breadcrumb"][1] == column_to_exclude
                ):
                    metadata["metadata"]["selected"] = False

    tap = TapPostgres(config=SAMPLE_CONFIG, catalog=tap_catalog)
    streams = tap.discover_streams()
    selected_stream = next(s for s in streams if s.selected is True)

    for row in selected_stream.get_records(context=None):
        assert column_to_exclude not in row


class TapTestSelectedColumnsOnly(TapTestTemplate):
    name = "selected_columns_only"
    table_name = TABLE_NAME_SELECTED_COLUMNS_ONLY

    def test(self):
        selected_columns_only_test(self.tap, self.table_name)
