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


def test_connection_parameters_from_sqlalchemy_url_defaults_port_and_keeps_ssl_paths(
    tmp_path: Path,
) -> None:
    rootcert = tmp_path / "root.crt"
    rootcert.write_text("CA", encoding="utf-8")

    cfg = {
        "sqlalchemy_url": (
            f"postgresql://user:pass@localhost/mydb?sslmode=verify-full&sslrootcert={rootcert}"
        ),
    }

    parameters = ConnectionParameters.from_tap_config(cfg)

    assert parameters.port == 5432  # noqa: PLR2004
    assert parameters.options == {
        "application_name": "tap_postgres",
        "sslmode": "verify-full",
        "sslrootcert": str(rootcert),
    }


def test_connection_parameters_renders_as_sqlalchemy_url(tmp_path: Path) -> None:
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


def test_connection_parameters_renders_as_psycopg2_dsn(tmp_path: Path) -> None:
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
