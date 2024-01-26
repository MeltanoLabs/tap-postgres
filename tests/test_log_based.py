import json

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import BIGINT, TEXT

from tap_postgres.tap import TapPostgres
from tests.test_core import PostgresTestRunner

LOG_BASED_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres",
}


def test_null_append():
    """LOG_BASED syncs failed with string property types. (issue #294).

    This test checks that even when a catalog contains properties with types represented
    as strings (ex: "object") instead of arrays (ex: ["object"] or ["object", "null"]),
    LOG_BASED replication can still append the "null" option to a property's type.
    """
    table_name = "test_null_append"
    engine = sa.create_engine("postgresql://postgres:postgres@localhost:5434/postgres")

    metadata_obj = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id", BIGINT, primary_key=True),
        sa.Column("data", TEXT, nullable=True),
    )
    with engine.connect() as conn:
        table.drop(conn, checkfirst=True)
        metadata_obj.create_all(conn)
        insert = table.insert().values(id=123, data="hello world")
        conn.execute(insert)
    tap = TapPostgres(config=LOG_BASED_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)
    altered_table_name = f"public-{table_name}"
    for stream in tap_catalog["streams"]:
        if stream.get("stream") and altered_table_name not in stream["stream"]:
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = False
        else:
            stream["replication_method"] = "LOG_BASED"
            stream["replication_key"] = "_sdc_lsn"
            stream["schema"]["properties"]["data"]["type"] = "string"
            for metadata in stream["metadata"]:
                metadata["metadata"]["selected"] = True
                if metadata["breadcrumb"] == []:
                    metadata["metadata"]["replication-method"] = "LOG_BASED"

    test_runner = PostgresTestRunner(
        tap_class=TapPostgres, config=LOG_BASED_CONFIG, catalog=tap_catalog
    )
    test_runner.sync_all()
