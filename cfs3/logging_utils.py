import logging
import sys
import os

def get_logger(name: str = None, level: int = logging.DEBUG) -> logging.Logger:
    """
    Return a consistent logger instance.

    - Defaults to DEBUG level.
    - Adds a StreamHandler if no handlers exist on this logger and not running under pytest.
    - Under pytest, logs are captured by pytest; no handlers are added or removed.
    - Always propagates so higher-level frameworks can capture messages.
    """
    logger = logging.getLogger(name or "cfs3")
    logger.setLevel(level)

    # Detect pytest environment
    under_pytest = "PYTEST_CURRENT_TEST" in os.environ

    # Only attach a handler if no handlers exist on this logger and not running under pytest
    if not under_pytest and len(logger.handlers) == 0:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Always propagate to allow pytest or parent loggers to capture messages
    logger.propagate = True

    return logger
