"""Helpers for building Postgres connection parameters.

This module is intentionally side-effect-light and testable. It centralizes the
mapping from tap config to the connection parameters used by libpq-compatible
clients (SQLAlchemy/psycopg2).
"""

from __future__ import annotations

from dataclasses import dataclass
from os import chmod, path
from pathlib import Path
from typing import TYPE_CHECKING, Any

from psycopg2.extensions import make_dsn
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url

if TYPE_CHECKING:
    from collections.abc import Mapping

APPLICATION_NAME = "tap_postgres"


@dataclass(frozen=True, slots=True)
class ConnectionParameters:
    """Postgres connection parameters."""

    host: str
    port: int
    database: str
    user: str
    password: str
    options: dict[str, str]

    @classmethod
    def from_tap_config(cls, config: Mapping[str, Any]) -> ConnectionParameters:
        """Build the connection parameters from tap config.

        Args:
            config: Tap config.

        Returns:
            ConnectionParameters with base connection fields and SQLAlchemy query parameters.
        """
        if sqlalchemy_url := config.get("sqlalchemy_url"):
            url = make_url(sqlalchemy_url)

            if (
                url.host is None
                or url.database is None
                or url.username is None
                or url.password is None
            ):
                msg = "sqlalchemy_url must include host, database, username, and password"
                raise ValueError(msg)

            return cls(
                host=url.host,
                port=int(url.port or 5432),
                database=url.database,
                user=url.username,
                password=str(url.password),
                options=_build_options_from_sqlalchemy_url(sqlalchemy_url),
            )

        return cls(
            host=config["host"],
            port=int(config["port"]),
            database=config["database"],
            user=config["user"],
            password=config["password"],
            options=_build_options_from_tap_config(config),
        )

    def with_host_and_port(self, host: str, port: int) -> ConnectionParameters:
        """Return a new ConnectionParameters with the given host and port.

        Args:
            host: New host value.
            port: New port value.

        Returns:
            New ConnectionParameters with updated host and port.
        """
        return ConnectionParameters(
            host=host,
            port=port,
            database=self.database,
            user=self.user,
            password=self.password,
            options=self.options,
        )

    def render_as_sqlalchemy_url(self) -> str:
        """Render connection parameters as a SQLAlchemy URL string.

        Returns:
            SQLAlchemy URL string.
        """
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=self.options,
        ).render_as_string(hide_password=False)

    def render_as_psycopg2_dsn(self) -> str:
        """Render connection parameters as a Psycopg2 DSN string.

        Returns:
            PostgreSQL DSN string.
        """
        return make_dsn(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            **self.options,
        )


def _build_options_from_tap_config(config: Mapping[str, Any]) -> dict[str, str]:
    """Build the postgresql options dict from config.

    Args:
        config: Tap config.

    Returns:
        A dict suitable for a PostgreSQL DSN query string.
    """
    options: dict[str, str] = {
        "application_name": APPLICATION_NAME,
    }

    storage_dir = Path(config.get("ssl_storage_directory", ".secrets"))

    # ssl_enable is for verifying the server's identity to the client.
    if config.get("ssl_enable"):
        ssl_mode = config["ssl_mode"]
        options["sslmode"] = ssl_mode

        if ssl_mode in ("verify-ca", "verify-full") and config.get("ssl_certificate_authority"):
            options["sslrootcert"] = _filepath_or_certificate(
                value=config["ssl_certificate_authority"],
                alternative_path=storage_dir / "root.crt",
            )

    # ssl_client_certificate_enable is for verifying the client's identity to the server.
    if config.get("ssl_client_certificate_enable"):
        options["sslcert"] = _filepath_or_certificate(
            value=config["ssl_client_certificate"],
            alternative_path=storage_dir / "cert.crt",
        )
        options["sslkey"] = _filepath_or_certificate(
            value=config["ssl_client_private_key"],
            alternative_path=storage_dir / "pkey.key",
            restrict_permissions=True,
        )

    return options


def _filepath_or_certificate(
    value: str,
    alternative_path: Path,
    restrict_permissions: bool = False,
) -> str:
    """Provide the appropriate key-value pair based on a filepath or raw value.

    For SSL configuration options, support is provided for either raw values in
    .env file or filepaths to a file containing a certificate. This function
    attempts to parse a value as a filepath, and if no file is found, assumes the
    value is a certificate and creates a file named `alternative_path` to store the
    file.

    Args:
        value: Either a filepath or a raw value to be written to a file.
        alternative_path: The filename to use in case `value` is not a filepath.
        restrict_permissions: Whether to restrict permissions on a newly created
            file. On UNIX systems, private keys cannot have public access.

    Returns:
        Filepath for a certificate/key value
    """
    if path.isfile(value):
        return value

    Path(alternative_path).parent.mkdir(parents=True, exist_ok=True)

    with open(alternative_path, "wb") as alternative_file:
        alternative_file.write(value.encode("utf-8"))
    if restrict_permissions:
        chmod(alternative_path, 0o600)

    return str(alternative_path)


def _build_options_from_sqlalchemy_url(sqlalchemy_url: str) -> dict[str, str]:
    """Build connection options from a SQLAlchemy URL.

    Only a limited set of libpq/psycopg2 options are supported here.
    `application_name` is always enforced and cannot be overridden.
    """
    url = make_url(sqlalchemy_url)

    options: dict[str, str] = {
        "application_name": APPLICATION_NAME,
    }
    for key, value in url.query.items():
        if value is not None:
            options[key] = str(value)
    return options
