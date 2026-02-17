"""
Structured logging — wraps structlog for consistent JSON/console output.
Every module calls: logger = get_logger(__name__)
"""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from rich.console import Console
from rich.logging import RichHandler

_console = Console(stderr=True)
_configured = False


def configure_logging(level: str = "INFO", fmt: str = "console") -> None:
    """
    Call once at startup (app.py / cli.py).
    fmt: "console" (rich, human-readable) | "json" (structured, for log aggregators)
    """
    global _configured
    if _configured:
        return
    _configured = True

    log_level = getattr(logging, level.upper(), logging.INFO)

    if fmt == "json":
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )
    else:
        logging.basicConfig(
            level=log_level,
            format="%(message)s",
            handlers=[RichHandler(console=_console, rich_tracebacks=True, markup=True)],
        )
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
        )


def get_logger(name: str, **initial_ctx: Any) -> structlog.BoundLogger:
    """Return a bound logger with optional initial context values."""
    return structlog.get_logger(name).bind(**initial_ctx)
