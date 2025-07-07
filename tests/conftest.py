"""Test Configuration."""


def pytest_sessionfinish(session, exitstatus):
    """Session Finish."""
    import logging  # noqa: PLC0415

    loggers: list[logging.Logger] = [
        logging.getLogger(),
        *list(logging.Logger.manager.loggerDict.values()),
    ]

    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
