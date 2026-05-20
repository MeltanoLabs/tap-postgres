"""Test Configuration."""

import sys

import pytest


def pytest_sessionfinish(session: pytest.Session, exitstatus: int | pytest.ExitCode) -> None:
    """Session Finish."""
    import logging  # noqa: PLC0415

    loggers = [
        logging.getLogger(),
        *list(logging.Logger.manager.loggerDict.values()),
    ]

    for logger in loggers:
        if isinstance(logger, logging.Logger):
            handlers = getattr(logger, "handlers", [])
            for handler in handlers:
                logger.removeHandler(handler)


def pytest_configure(config: pytest.Config):
    if sys.version_info < (3, 11):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.10 will reach its end of life on 2026-10:FutureWarning",
        )
