"""Logging configuration: console + rotating file."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure root logger with console and file handlers."""
    logger = logging.getLogger("wb_chat_bot")
    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # File handler with rotation (10 MB, keep 5 files)
    file_handler = RotatingFileHandler(
        "logs/app.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
