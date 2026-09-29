"""Prometheus metrics for the twin-core service boundary."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest


class TwinMetrics:
    """Keep metric definitions and updates in one place."""

    def __init__(self) -> None:
        """Create a private registry for this service and its tests."""
        self.registry = CollectorRegistry()
        self.ticks = Counter(
            "well_sim_twin_ticks_total",
            "Fixed-step service ticks completed",
            registry=self.registry,
        )
        self.skipped_ticks = Counter(
            "well_sim_twin_skipped_ticks_total",
            "Fixed-step ticks skipped after the catch-up limit",
            registry=self.registry,
        )
        self.lag_seconds = Gauge(
            "well_sim_twin_simulation_lag_seconds",
            "Scheduler lag before the current tick pass",
            registry=self.registry,
        )
        self.active_wellheads = Gauge(
            "well_sim_twin_active_wellheads",
            "Active wellheads loaded from the asset table",
            registry=self.registry,
        )
        self.invalid_parameters = Gauge(
            "well_sim_twin_invalid_parameter_wellheads",
            "Wellheads whose model parameters failed validation",
            registry=self.registry,
        )
        self.database_errors = Counter(
            "well_sim_twin_database_errors_total",
            "Historian reads that failed",
            ["operation"],
            registry=self.registry,
        )
        self.rejected_reads = Counter(
            "well_sim_twin_rejected_reads_total",
            "Telemetry reads rejected because all database read slots were busy",
            registry=self.registry,
        )
        self.ready = Gauge(
            "well_sim_twin_ready",
            "One when service reads and the tick loop are ready",
            registry=self.registry,
        )

    def render(self) -> bytes:
        """Return the Prometheus text format."""
        return generate_latest(self.registry)
