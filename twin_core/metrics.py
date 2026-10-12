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
        self.model_steps = Counter(
            "well_sim_twin_model_steps_total",
            "One-second physical model steps completed across all wells",
            registry=self.registry,
        )
        self.model_step_errors = Counter(
            "well_sim_twin_model_step_errors_total",
            "Physical model steps that failed validation or calculation",
            registry=self.registry,
        )
        self.running_wellheads = Gauge(
            "well_sim_twin_running_wellheads",
            "Active wellheads with a running physical model",
            registry=self.registry,
        )
        self.skipped_ticks = Counter(
            "well_sim_twin_skipped_ticks_total",
            "Fixed-step ticks skipped after the catch-up limit",
            registry=self.registry,
        )
        self.clock_adjustments = Counter(
            "well_sim_twin_clock_adjustments_total",
            "Times tick time was realigned with a wall clock that jumped",
            ["direction"],
            registry=self.registry,
        )
        self.snapshot_writes = Counter(
            "well_sim_twin_snapshot_writes_total",
            "Well state rows saved for recovery",
            registry=self.registry,
        )
        self.snapshot_write_failures = Counter(
            "well_sim_twin_snapshot_write_failures_total",
            "Fleet snapshot transactions that failed",
            registry=self.registry,
        )
        self.snapshots_replaced = Counter(
            "well_sim_twin_snapshots_replaced_total",
            "Pending fleet snapshots replaced by newer copies",
            registry=self.registry,
        )
        self.last_snapshot_age_seconds = Gauge(
            "well_sim_twin_last_snapshot_age_seconds",
            "Wall age of the last saved fleet snapshot",
            registry=self.registry,
        )
        self.recoveries = Counter(
            "well_sim_twin_recoveries_total",
            "Recovery and clock events by outcome",
            ["outcome"],
            registry=self.registry,
        )
        self.recovery_events_dropped = Counter(
            "well_sim_twin_recovery_events_dropped_total",
            "Recovery events rejected because the queue was full",
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
