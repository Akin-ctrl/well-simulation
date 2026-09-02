"""Prometheus metrics for the telemetry services.

ADR 0029 asks for Prometheus-compatible metrics. These are the numbers that
answer the questions the audit could not: is telemetry arriving, is it fresh,
and is anything failing quietly.

Defined in one module so the metric names cannot drift between the service that
records them and the one that reads them.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Ingestion --------------------------------------------------------------

READINGS_WRITTEN = Counter(
    "wellhead_readings_written_total",
    "Parameter readings inserted into the historian",
)

POLLS_SKIPPED_STALE = Counter(
    "wellhead_polls_skipped_stale_total",
    "Poll cycles skipped because the gateway heartbeat had not advanced",
)

MODBUS_READ_ERRORS = Counter(
    "wellhead_modbus_read_errors_total",
    "Modbus register reads that returned an error",
    ["kind"],
)

INGESTION_RECONNECTS = Counter(
    "wellhead_ingestion_reconnects_total",
    "Times the ingestion loop restarted after a failure",
)

POLL_DURATION = Histogram(
    "wellhead_poll_duration_seconds",
    "Time to read every mapped register once",
)

TELEMETRY_AGE = Gauge(
    "wellhead_telemetry_age_seconds",
    "Age of the gateway heartbeat as seen by ingestion",
)

# Gateway ----------------------------------------------------------------

BATCHES_APPLIED = Counter(
    "wellhead_batches_applied_total",
    "Telemetry batches written into the register image",
)

BATCH_ERRORS = Counter(
    "wellhead_batch_errors_total",
    "Telemetry batches that could not be applied",
)

# Shared -----------------------------------------------------------------

METADATA_RELOADS = Counter(
    "wellhead_metadata_reloads_total",
    "Metadata reloads, by whether the mappings changed",
    ["result"],
)
