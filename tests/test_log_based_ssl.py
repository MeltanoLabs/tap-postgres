from tap_postgres.tap import TapPostgres

LOG_BASED_SSL_CONFIG = {
    "host": "localhost",
    "port": 5435,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres",
    "ssl_enable": True,
    "ssl_client_certificate_enable": True,
    "ssl_mode": "verify-full",
    "ssl_certificate_authority": "./ssl/root.crt",
    "ssl_client_certificate": "./ssl/cert.crt",
    "ssl_client_private_key": "./ssl/pkey.key",
    "default_replication_method": "LOG_BASED",
}


def test_ssl():
    """We expect the SSL environment to already be up"""
    tap = TapPostgres(config=LOG_BASED_SSL_CONFIG)
    tap.sync_all()
