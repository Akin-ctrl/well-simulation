"""Emit structured logs without exposing database settings or request data."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonLogFormatter(logging.Formatter):
    """Render one log record as one JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        """Include operational fields and a UTC timestamp."""
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "service": "twin-core",
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("wellhead_id", "skipped_ticks", "lag_seconds"):
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        return json.dumps(payload)


def configure_logging() -> None:
    """Install the service's structured stdout logger."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
