"""Tests standard tap features using the built-in SDK tests library."""

from tap_postgres.tap import TapPostgres

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG = {
    "dialect+driver": "postgresql+psycopg2",
    "host": "localhost",
    "user": "postgres",
    "password": "postgres",
    "database": "postgres",
    "port": 5433,
    "ssl_enable": True,
    "ssl_client_certificate_enable": True,
    "ssl_mode": "verify-full",
    "ssl_certificate_authority": "./ssl/root.crt",
    "ssl_client_certificate": "./ssl/cert.crt",
    "ssl_client_private_key": "./ssl/pkey.key",
}


def test_ssl():
    """We expect the SSL environment to already be up"""
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap.sync_all()
