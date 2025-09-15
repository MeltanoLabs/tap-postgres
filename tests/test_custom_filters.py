"""Tests for custom WHERE conditions functionality."""

import datetime
import pytest
import sqlalchemy as sa
from faker import Faker
from singer_sdk.singerlib import Catalog, StreamMetadata
from singer_sdk.testing.runners import TapTestRunner

from tap_postgres.tap import TapPostgres
from tests.settings import DB_SCHEMA_NAME, DB_SQLALCHEMY_URL


class PostgresTestRunner(TapTestRunner):
    """Custom test runner for Postgres tests."""
    
    pass


def setup_test_tables(sqlalchemy_url):
    """Set up test tables for custom filters testing."""
    engine = sa.create_engine(sqlalchemy_url, future=True)
    fake = Faker()
    
    metadata_obj = sa.MetaData()
    
    # Create organizations table
    organizations_table = sa.Table(
        "test_organizations",
        metadata_obj,
        sa.Column("id", sa.String(50), primary_key=True),
        sa.Column("name", sa.String(100)),
        sa.Column("active", sa.Boolean(), default=True),
    )
    
    # Create users table
    users_table = sa.Table(
        "test_users",
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(100)),
        sa.Column("email", sa.String(100)),
        sa.Column("organizationId", sa.String(50)),
        sa.Column("created_at", sa.DateTime()),
    )
    
    # Create orders table
    orders_table = sa.Table(
        "test_orders",
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("user_id", sa.Integer),
        sa.Column("organizationId", sa.String(50)),
        sa.Column("total", sa.Numeric(10, 2)),
        sa.Column("status", sa.String(20)),
        sa.Column("created_at", sa.DateTime()),
    )
    
    # Create products table
    products_table = sa.Table(
        "test_products",
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(100)),
        sa.Column("price", sa.Numeric(10, 2)),
        sa.Column("active", sa.Boolean(), default=True),
        sa.Column("organizationId", sa.String(50)),
    )
    
    with engine.begin() as conn:
        metadata_obj.drop_all(conn)
        metadata_obj.create_all(conn)
        
        # Insert test data
        # Organizations
        orgs = [
            {"id": "org_123", "name": "Test Org 1", "active": True},
            {"id": "org_456", "name": "Test Org 2", "active": True},
            {"id": "org_789", "name": "Test Org 3", "active": False},
        ]
        conn.execute(organizations_table.insert().values(orgs))
        
        # Users
        users = []
        for org in orgs:
            for i in range(5):
                users.append({
                    "name": fake.name(),
                    "email": fake.email(),
                    "organizationId": org["id"],
                    "created_at": fake.date_time_between(
                        start_date=datetime.datetime(2023, 1, 1),
                        end_date=datetime.datetime(2024, 12, 31)
                    ),
                })
        conn.execute(users_table.insert().values(users))
        
        # Orders
        orders = []
        for i in range(30):
            orders.append({
                "user_id": (i % 15) + 1,  # References users 1-15
                "organizationId": orgs[i % 3]["id"],
                "total": fake.pydecimal(left_digits=3, right_digits=2, positive=True),
                "status": fake.random_element(["pending", "completed", "cancelled"]),
                "created_at": fake.date_time_between(
                    start_date=datetime.datetime(2023, 1, 1),
                    end_date=datetime.datetime(2024, 12, 31)
                ),
            })
        conn.execute(orders_table.insert().values(orders))
        
        # Products
        products = []
        for org in orgs:
            for i in range(10):
                products.append({
                    "name": fake.word().capitalize() + " Product",
                    "price": fake.pydecimal(left_digits=2, right_digits=2, positive=True),
                    "active": fake.boolean(chance_of_getting_true=80),
                    "organizationId": org["id"],
                })
        conn.execute(products_table.insert().values(products))


def teardown_test_tables(sqlalchemy_url):
    """Tear down test tables."""
    engine = sa.create_engine(sqlalchemy_url, future=True)
    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS test_orders"))
        conn.execute(sa.text("DROP TABLE IF EXISTS test_users"))
        conn.execute(sa.text("DROP TABLE IF EXISTS test_organizations"))
        conn.execute(sa.text("DROP TABLE IF EXISTS test_products"))


@pytest.fixture(scope="module")
def test_tables():
    """Fixture to set up and tear down test tables."""
    setup_test_tables(DB_SQLALCHEMY_URL)
    yield
    teardown_test_tables(DB_SQLALCHEMY_URL)


def test_custom_where_conditions(test_tables):
    """Test custom WHERE conditions functionality."""
    config = {
        "sqlalchemy_url": DB_SQLALCHEMY_URL,
        "custom_where_conditions": {
            "public-test_users": "\"organizationId\" = 'org_123'",
            "public-test_orders": "status != 'cancelled'",
            "public-test_products": "active = true AND price > 10",
        }
    }
    
    tap = TapPostgres(config=config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    
    # Select only our test tables
    for stream in tap_catalog.streams:
        stream_name = stream.stream.split("-")[-1] if stream.stream else ""
        if stream_name.startswith("test_"):
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "FULL_TABLE"
        else:
            for metadata in stream.metadata.values():
                metadata.selected = False
    
    test_runner = PostgresTestRunner(
        tap_class=TapPostgres, config=config, catalog=tap_catalog
    )
    test_runner.sync_all()
    
    # Verify that WHERE conditions were applied
    users_stream = f"{DB_SCHEMA_NAME}-test_users"
    orders_stream = f"{DB_SCHEMA_NAME}-test_orders"
    products_stream = f"{DB_SCHEMA_NAME}-test_products"
    
    # Check users - should only have org_123 users
    if users_stream in test_runner.records:
        for record in test_runner.records[users_stream]:
            assert record["organizationId"] == "org_123", \
                f"Found user with organizationId {record['organizationId']}, expected org_123"
    
    # Check orders - should not have cancelled orders
    if orders_stream in test_runner.records:
        for record in test_runner.records[orders_stream]:
            assert record["status"] != "cancelled", \
                "Found cancelled order, which should be filtered out"
    
    # Check products - should only have active products with price > 10
    if products_stream in test_runner.records:
        for record in test_runner.records[products_stream]:
            assert record["active"] is True, \
                f"Found inactive product: {record}"
            assert float(record["price"]) > 10, \
                f"Found product with price <= 10: {record['price']}"


def test_custom_filters_with_replication_key(test_tables):
    """Test that custom filters work alongside replication key filtering."""
    config = {
        "sqlalchemy_url": DB_SQLALCHEMY_URL,
        "start_date": "2024-01-01T00:00:00Z",
        "custom_where_conditions": {
            "public-test_orders": "status = 'completed'"
        }
    }
    
    tap = TapPostgres(config=config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    
    # Select only test_orders and set incremental replication
    for stream in tap_catalog.streams:
        stream_name = stream.stream.split("-")[-1] if stream.stream else ""
        if stream_name == "test_orders":
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "INCREMENTAL"
                    metadata.replication_key = "created_at"
        else:
            for metadata in stream.metadata.values():
                metadata.selected = False
    
    test_runner = PostgresTestRunner(
        tap_class=TapPostgres, config=config, catalog=tap_catalog
    )
    test_runner.sync_all()
    
    # Verify that both custom WHERE and replication key filtering were applied
    orders_stream = f"{DB_SCHEMA_NAME}-test_orders"
    
    if orders_stream in test_runner.records:
        for record in test_runner.records[orders_stream]:
            # Check custom WHERE condition
            assert record["status"] == "completed", \
                f"Found order with status {record['status']}, expected only completed"
            
            # Check replication key filter
            created_at = datetime.datetime.fromisoformat(
                record["created_at"].replace("T", " ").replace("Z", "")
            )
            assert created_at >= datetime.datetime(2024, 1, 1), \
                f"Found order created before 2024-01-01: {record['created_at']}"


def test_no_custom_filters():
    """Test that tap works normally when no custom filters are configured."""
    config = {
        "sqlalchemy_url": DB_SQLALCHEMY_URL,
    }
    
    tap = TapPostgres(config=config)
    tap_catalog = Catalog.from_dict(tap.catalog_dict)
    
    # Select a test table
    for stream in tap_catalog.streams:
        stream_name = stream.stream.split("-")[-1] if stream.stream else ""
        if stream_name == "test_products":
            for metadata in stream.metadata.values():
                metadata.selected = True
                if isinstance(metadata, StreamMetadata):
                    metadata.forced_replication_method = "FULL_TABLE"
        else:
            for metadata in stream.metadata.values():
                metadata.selected = False
    
    test_runner = PostgresTestRunner(
        tap_class=TapPostgres, config=config, catalog=tap_catalog
    )
    
    # Should not raise any errors
    test_runner.sync_all()
    
    products_stream = f"{DB_SCHEMA_NAME}-test_products"
    
    # Should have all products (no filtering)
    if products_stream in test_runner.records:
        assert len(test_runner.records[products_stream]) == 30, \
            f"Expected 30 products, got {len(test_runner.records[products_stream])}"