from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path

from tap_postgres.connection_parameters import ConnectionParameters


def _base_config(tmp_path: Path) -> dict:
    return {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "ssl_enable": False,
        "ssl_client_certificate_enable": False,
        "ssl_mode": "verify-full",
        "ssl_storage_directory": str(tmp_path / "secrets"),
    }


def test_base_connection_parameters(tmp_path: Path) -> None:
    parameters = ConnectionParameters.from_tap_config(_base_config(tmp_path))

    assert asdict(parameters) == {
        "host": "localhost",
        "port": 5432,
        "database": "postgres",
        "user": "postgres",
        "password": "postgres",
        "options": {"application_name": "tap_postgres"},
    }


def test_connection_parameters_ssl_require_sets_sslmode_only(
    tmp_path: Path,
) -> None:
    cfg = _base_config(tmp_path)
    cfg.update({"ssl_enable": True, "ssl_mode": "require"})

    parameters = ConnectionParameters.from_tap_config(cfg)
    assert parameters.options == {
        "application_name": "tap_postgres",
        "sslmode": "require",
    }


def test_connection_parameters_writes_rootcert_when_verify_full(
    tmp_path: Path,
) -> None:
    cfg = _base_config(tmp_path)
    cfg.update(
        {
            "ssl_enable": True,
            "ssl_mode": "verify-full",
            "ssl_certificate_authority": "---ROOTCERT---",
        }
    )

    parameters = ConnectionParameters.from_tap_config(cfg)
    assert parameters.options["sslmode"] == "verify-full"

    rootcert_path = Path(parameters.options["sslrootcert"])
    assert rootcert_path.is_file()
    assert rootcert_path.read_text(encoding="utf-8") == "---ROOTCERT---"


def test_connection_parameters_uses_existing_ca_path(tmp_path: Path) -> None:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text("CA", encoding="utf-8")

    cfg = _base_config(tmp_path)
    cfg.update(
        {
            "ssl_enable": True,
            "ssl_mode": "verify-ca",
            "ssl_certificate_authority": str(ca_path),
        }
    )

    parameters = ConnectionParameters.from_tap_config(cfg)
    assert parameters.options["sslrootcert"] == str(ca_path)


def test_connection_parameters_writes_client_cert_and_key(tmp_path: Path) -> None:
    cfg = _base_config(tmp_path)
    cfg.update(
        {
            "ssl_client_certificate_enable": True,
            "ssl_client_certificate": "---CERT---",
            "ssl_client_private_key": "---PKEY---",
        }
    )

    parameters = ConnectionParameters.from_tap_config(cfg)

    cert_path = Path(parameters.options["sslcert"])
    key_path = Path(parameters.options["sslkey"])

    assert cert_path.is_file()
    assert cert_path.read_text(encoding="utf-8") == "---CERT---"

    assert key_path.is_file()
    assert key_path.read_text(encoding="utf-8") == "---PKEY---"

    # Private keys must not be world-readable.
    mode = os.stat(key_path).st_mode & 0o777
    assert mode == 0o600  # noqa: PLR2004


def test_connection_parameters_from_sqlalchemy_url_parses_fields(
    tmp_path: Path,
) -> None:
    cfg = {
        "sqlalchemy_url": (
            "postgresql://user:pass@db.example.com:5439/mydb"
            "?sslmode=require&application_name=another-name&connect_timeout=5"
        ),
        # These would normally enable SSL, but must be ignored when sqlalchemy_url is used.
        "ssl_enable": True,
        "ssl_mode": "verify-full",
        "ssl_certificate_authority": "---ROOTCERT---",
        "ssl_storage_directory": str(tmp_path / "secrets"),
    }

    parameters = ConnectionParameters.from_tap_config(cfg)

    assert asdict(parameters) == {
        "host": "db.example.com",
        "port": 5439,
        "database": "mydb",
        "user": "user",
        "password": "pass",
        "options": {
            "application_name": "another-name",
            "sslmode": "require",
            "connect_timeout": "5",
        },
    }


def test_from_sqlalchemy_url_default_port() -> None:
    cfg = {
        "sqlalchemy_url": ("postgresql://user:pass@localhost/mydb"),
    }

    parameters = ConnectionParameters.from_tap_config(cfg)
    assert parameters.port == 5432  # noqa: PLR2004


def test_from_sqlalchemy_url_and_preserves_ssl_paths(tmp_path: Path) -> None:
    rootcert = tmp_path / "root.crt"
    rootcert.write_text("CA", encoding="utf-8")

    cfg = {
        "sqlalchemy_url": (
            f"postgresql://user:pass@localhost/mydb?sslmode=verify-full&sslrootcert={rootcert}"
        ),
    }

    parameters = ConnectionParameters.from_tap_config(cfg)
    assert parameters.options == {
        "application_name": "tap_postgres",
        "sslmode": "verify-full",
        "sslrootcert": str(rootcert),
    }


def test_renders_as_sqlalchemy_url(tmp_path: Path) -> None:
    cfg = _base_config(tmp_path)
    cfg.update(
        {
            "ssl_enable": True,
            "ssl_mode": "require",
        }
    )

    parameters = ConnectionParameters.from_tap_config(cfg)
    sqlalchemy_url = parameters.render_as_sqlalchemy_url()

    assert sqlalchemy_url == (
        "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
        "?application_name=tap_postgres&sslmode=require"
    )


def test_renders_as_psycopg2_dsn(tmp_path: Path) -> None:
    cfg = _base_config(tmp_path)
    cfg.update(
        {
            "ssl_enable": True,
            "ssl_mode": "require",
        }
    )

    parameters = ConnectionParameters.from_tap_config(cfg)
    dsn = parameters.render_as_psycopg2_dsn()

    assert dsn == (
        "host=localhost port=5432 dbname=postgres user=postgres password=postgres "
        "application_name=tap_postgres sslmode=require"
    )


def test_with_host_and_port():
    """Unit test for ConnectionParameters.with_host_and_port method."""
    # Create original connection parameters (pointing to remote database)
    original = ConnectionParameters(
        host="remote-db.example.com",
        port=5432,
        database="testdb",
        user="testuser",
        password="testpass",
        options={"sslmode": "require", "application_name": "tap_postgres"},
    )

    ssh_tunnel_host = "127.0.0.1"
    ssh_tunnel_port = 12345

    # Simulate what ssh_tunnel_connect does: update to tunnel's local bind address
    updated = original.with_host_and_port(
        host=ssh_tunnel_host,  # tunnel's local_bind_host
        port=ssh_tunnel_port,  # tunnel's local_bind_port
    )

    assert updated.host == ssh_tunnel_host
    assert updated.port == ssh_tunnel_port

    # Verify other parameters are preserved
    assert updated.database == original.database == "testdb"
    assert updated.user == original.user == "testuser"
    assert updated.password == original.password == "testpass"
    assert (
        updated.options
        == original.options
        == {"sslmode": "require", "application_name": "tap_postgres"}
    )

    # Verify original parameters are unchanged (immutability check)
    assert original.host == "remote-db.example.com"
    assert original.port == 5432  # noqa: PLR2004

    # Verify the connection strings use the tunnel address
    sqlalchemy_url = updated.render_as_sqlalchemy_url()
    psycopg2_dsn = updated.render_as_psycopg2_dsn()

    assert ssh_tunnel_host in sqlalchemy_url
    assert str(ssh_tunnel_port) in sqlalchemy_url
    assert original.host not in sqlalchemy_url

    assert f"host={ssh_tunnel_host}" in psycopg2_dsn
    assert f"port={ssh_tunnel_port}" in psycopg2_dsn
    assert original.host not in psycopg2_dsn
