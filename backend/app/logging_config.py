"""JSON logging shared by the API and simulator subsystems."""

from __future__ import annotations

import logging
from logging.config import dictConfig


def configure_logging() -> None:
    """Install one structured stdout handler for application log records."""
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {
                    "()": "pythonjsonlogger.json.JsonFormatter",
                    "fmt": "%(asctime)s %(levelname)s %(name)s %(message)s",
                    "rename_fields": {
                        "asctime": "timestamp",
                        "levelname": "level",
                        "name": "logger",
                    },
                }
            },
            "handlers": {
                "stdout": {
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                    "stream": "ext://sys.stdout",
                }
            },
            "root": {"handlers": ["stdout"], "level": "INFO"},
        }
    )
    logging.captureWarnings(True)
