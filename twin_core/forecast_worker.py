"""Run one durable scheduled forecast at a time outside the live tick loop."""

from __future__ import annotations

import logging
import threading
import time

from twin_core.database import DatabaseUnavailableError
from twin_core.forecast_store import ForecastClaimError, ForecastStore
from twin_core.runtime import (
    ProjectionBusyError,
    ProjectionUnavailableError,
    TwinRuntime,
)

logger = logging.getLogger(__name__)
POLL_SECONDS = 1.0
SHUTDOWN_SECONDS = 5.0


class ForecastWorker:
    """Bound work and leave interrupted claims for explicit expiry, never replay."""

    def __init__(self, runtime: TwinRuntime, store: ForecastStore) -> None:
        """Reuse the runtime's current-state checks and projection capacity."""
        self.runtime = runtime
        self.store = store
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="forecast-scheduler", daemon=True
        )

    def start(self) -> None:
        """Start after the model service has initialized its dependencies."""
        self._thread.start()

    def stop(self) -> None:
        """Stop future claims and allow a short in-progress calculation to finish."""
        self._stop.set()
        self._thread.join(timeout=SHUTDOWN_SECONDS)
        self.runtime.metrics.forecast_scheduler_ready.set(0)
        if self._thread.is_alive():
            logger.warning("Scheduled forecast is still stopping; its claim can expire")

    def run_once(self) -> None:
        """Close expired claims, then process at most one due occurrence."""
        expired = self.store.expire()
        if expired:
            self.runtime.metrics.scheduled_forecasts.labels(outcome="expired").inc(
                expired
            )
        job = self.store.claim()
        self.runtime.metrics.forecast_scheduler_ready.set(1)
        self.runtime.metrics.forecast_scheduler_last_scan.set(time.time())
        if job is None:
            return
        if job.missed_intervals:
            self.runtime.metrics.forecast_intervals_missed.inc(job.missed_intervals)
        if job.status != "running":
            self.runtime.metrics.scheduled_forecasts.labels(outcome="skipped").inc()
            logger.info(
                "Scheduled forecast skipped",
                extra={"job_id": job.job_id, "reason": job.reason_code},
            )
            return
        reason: str | None = None
        try:
            self.runtime.create_projection(
                "forecast",
                job.wellhead_id,
                job.owner_user_id,
                job.horizon_seconds,
                job.step_seconds,
                scheduled_job=job,
            )
        except ForecastClaimError as exc:
            reason = str(exc)
        except ProjectionBusyError:
            reason = "projection_busy"
        except ProjectionUnavailableError:
            reason = "model_not_current"
        except DatabaseUnavailableError:
            reason = "database_unavailable"
        except (ArithmeticError, ValueError):
            reason = "projection_failed"
        outcome = self.store.fail(job, reason) if reason else "complete"
        self.runtime.metrics.scheduled_forecasts.labels(outcome=outcome).inc()
        logger.info(
            "Scheduled forecast finished",
            extra={"job_id": job.job_id, "outcome": outcome, "reason": reason},
        )

    def _run(self) -> None:
        """Keep transient failures visible without taking down live simulation."""
        while not self._stop.is_set():
            try:
                self.run_once()
            except (DatabaseUnavailableError, ValueError, ArithmeticError):
                self.runtime.metrics.forecast_scheduler_ready.set(0)
                logger.error("Scheduled forecast scan failed")
            self._stop.wait(POLL_SECONDS)
