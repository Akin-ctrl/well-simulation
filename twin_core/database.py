"""Read active assets and current simulator readings from the historian."""

from __future__ import annotations

import math
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

import psycopg2
import psycopg2.extensions

from twin_core.config import ServiceSettings

LATEST_TELEMETRY_SQL = """
SELECT pt.code, pt.canonical_unit,
       recent.raw_value, recent.timestamp_utc, recent.inserted_at
FROM wellhead AS wh
JOIN deviceParameterMapping AS mapping ON mapping.device_id = wh.device_id
JOIN parameterType AS pt ON pt.parameter_type_id = mapping.parameter_type_id
LEFT JOIN LATERAL (
    SELECT reading.raw_value, reading.timestamp_utc, reading.inserted_at
    FROM parameterReading AS reading
    WHERE reading.wellhead_id = wh.wellhead_id
      AND reading.mapping_id = mapping.mapping_id
    ORDER BY reading.timestamp_utc DESC, reading.parameter_reading_id DESC
    LIMIT 1
) AS recent ON TRUE
WHERE wh.wellhead_id = %s AND mapping.active = TRUE
ORDER BY pt.code
"""

ReadingQuality = Literal["fresh", "stale", "missing", "invalid", "clock_skew"]
SnapshotQuality = Literal[
    "fresh", "stale", "partial", "missing", "invalid", "clock_skew"
]


class DatabaseUnavailableError(RuntimeError):
    """The historian could not complete a bounded read."""


@dataclass(frozen=True)
class TelemetryReading:
    """One latest value, its unit, both timestamps, and its quality."""

    code: str
    unit: str | None
    value: float | None
    source_timestamp: datetime | None
    inserted_at: datetime | None
    age_seconds: float | None
    quality: ReadingQuality

    def to_payload(self) -> dict[str, object]:
        """Return the internal API representation."""
        return {
            "code": self.code,
            "unit": self.unit,
            "value": self.value,
            "sourceTimestamp": (
                self.source_timestamp.isoformat() if self.source_timestamp else None
            ),
            "insertedAt": self.inserted_at.isoformat() if self.inserted_at else None,
            "ageSeconds": self.age_seconds,
            "quality": self.quality,
        }


@dataclass(frozen=True)
class TelemetrySnapshot:
    """Latest readings for one active well, with completeness made explicit."""

    wellhead_id: int
    observed_at: datetime
    quality: SnapshotQuality
    complete: bool
    readings: tuple[TelemetryReading, ...]

    def to_payload(self) -> dict[str, object]:
        """Mark the existing simulator as the source of these readings."""
        return {
            "wellheadId": self.wellhead_id,
            "observedAt": self.observed_at.isoformat(),
            "source": "current_simulator_via_historian",
            "modelGenerated": False,
            "quality": self.quality,
            "complete": self.complete,
            "expectedParameterCount": len(self.readings),
            "availableParameterCount": sum(
                reading.value is not None for reading in self.readings
            ),
            "readings": [reading.to_payload() for reading in self.readings],
        }


def _ensure_utc(value: datetime) -> datetime:
    """Reject a database timestamp without a UTC offset."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Historian returned a timestamp without a time zone")
    return value.astimezone(timezone.utc)


def make_reading(
    code: str,
    unit: str | None,
    raw_value: float | None,
    source_timestamp: datetime | None,
    inserted_at: datetime | None,
    observed_at: datetime,
    freshness_seconds: int,
) -> TelemetryReading:
    """Classify a value without treating missing or bad data as zero."""
    if raw_value is None or source_timestamp is None:
        return TelemetryReading(code, unit, None, None, None, None, "missing")

    source = _ensure_utc(source_timestamp)
    insertion = _ensure_utc(inserted_at) if inserted_at else None
    age = (observed_at - source).total_seconds()
    if not math.isfinite(raw_value):
        return TelemetryReading(code, unit, None, source, insertion, age, "invalid")
    if age < -5:
        quality: ReadingQuality = "clock_skew"
    elif age > freshness_seconds:
        quality = "stale"
    else:
        quality = "fresh"
    return TelemetryReading(
        code, unit, float(raw_value), source, insertion, age, quality
    )


def make_snapshot(
    wellhead_id: int,
    rows: list[tuple[str, str | None, float | None, datetime | None, datetime | None]],
    observed_at: datetime,
    freshness_seconds: int,
) -> TelemetrySnapshot:
    """Build an honest current view from the mapped historian readings."""
    readings = tuple(
        make_reading(
            code, unit, value, source, inserted, observed_at, freshness_seconds
        )
        for code, unit, value, source, inserted in rows
    )
    qualities = {reading.quality for reading in readings}
    if "invalid" in qualities:
        quality: SnapshotQuality = "invalid"
    elif "clock_skew" in qualities:
        quality = "clock_skew"
    elif not readings or qualities == {"missing"}:
        quality = "missing"
    elif "missing" in qualities:
        quality = "partial"
    elif "stale" in qualities:
        quality = "stale"
    else:
        quality = "fresh"
    complete = bool(readings) and all(reading.value is not None for reading in readings)
    return TelemetrySnapshot(wellhead_id, observed_at, quality, complete, readings)


class HistorianReader:
    """Use a bounded, read-only database connection for each operation."""

    def __init__(self, settings: ServiceSettings) -> None:
        """Store the credentials without logging them."""
        self._settings = settings

    def _connect(self) -> psycopg2.extensions.connection:
        """Connect with server-enforced query and transaction limits."""
        return psycopg2.connect(
            host=self._settings.database_host,
            port=self._settings.database_port,
            dbname=self._settings.database_name,
            user=self._settings.database_user,
            password=self._settings.database_password,
            connect_timeout=3,
            options="-c statement_timeout=3000 -c default_transaction_read_only=on",
        )

    def active_wellhead_ids(self) -> list[int]:
        """Load active wellheads from the authoritative asset table."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cursor:
                cursor.execute(
                    "SELECT wellhead_id FROM wellhead "
                    "WHERE status = 'active' ORDER BY wellhead_id"
                )
                rows = cursor.fetchall()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not load active wellheads") from exc
        return [int(row[0]) for row in rows]

    def latest_telemetry(
        self, wellhead_id: int, freshness_seconds: int
    ) -> TelemetrySnapshot:
        """Read current mapped parameters without inventing a batch boundary."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cursor:
                cursor.execute(LATEST_TELEMETRY_SQL, (wellhead_id,))
                rows = cursor.fetchall()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not load latest telemetry") from exc
        return make_snapshot(
            wellhead_id, rows, datetime.now(timezone.utc), freshness_seconds
        )
