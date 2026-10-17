"""Poll durable command hints away from the tick, then apply one per fleet step."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable

from twin_core.control_store import CommandRef, ControlCommitUnknownError, ControlStore
from twin_core.database import DatabaseUnavailableError
from twin_core.metrics import TwinMetrics
from twin_core.state import ModelRegistry

logger = logging.getLogger(__name__)


class ControlInbox:
    """Bound memory and database work while keeping accepted commands durable."""

    def __init__(
        self,
        store: ControlStore,
        registry: ModelRegistry,
        metrics: TwinMetrics,
        reconciled: Callable[[int], None],
    ) -> None:
        """Keep one pending hint per well, never a mutable command payload."""
        self.store = store
        self.registry = registry
        self.metrics = metrics
        self.reconciled = reconciled
        self._pending: deque[CommandRef] = deque()
        self._uncertain: set[int] = set()
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="control-inbox", daemon=True
        )

    def start(self) -> None:
        """Start the single inbox worker for this model process."""
        self._thread.start()

    def stop(self) -> None:
        """Stop after the current bounded database operation."""
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            logger.error("Control inbox shutdown exceeded its wait budget")

    def notify(self) -> None:
        """Wake polling; an HTTP hint never changes active controls."""
        self._wake.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            self.scan()
            self._wake.wait(1)

    def scan(self) -> None:
        """Refresh oldest durable hints and reconcile any uncertain commit."""
        try:
            with self._lock:
                uncertain = tuple(self._uncertain)
            for wellhead_id in uncertain:
                if self.registry.get(wellhead_id) is None:
                    with self._lock:
                        self._uncertain.discard(wellhead_id)
                    continue
                active = self.store.read(wellhead_id)
                self.registry.install_controls(wellhead_id, active)
                self.reconciled(wellhead_id)
                with self._lock:
                    self._uncertain.discard(wellhead_id)
                self.metrics.control_outcomes.labels(outcome="reconciled").inc()
            expired = self.store.expire_queued()
            self.metrics.control_outcomes.labels(outcome="expired").inc(expired)
            commands, depth = self.store.scan()
            with self._lock:
                self._pending = deque(commands)
            self.metrics.control_queue_depth.set(depth)
            self.metrics.control_backlog.set(max(0, depth - 100))
            self.metrics.control_inbox_ready.set(1)
        except (DatabaseUnavailableError, ValueError):
            self.metrics.database_errors.labels(operation="control_inbox").inc()
            self.metrics.control_inbox_ready.set(0)
            logger.warning("Control inbox unavailable")

    def apply_one(self) -> None:
        """Apply at most one prepared command before a model step."""
        with self._lock:
            ref = self._pending.popleft() if self._pending else None
            if ref is None or ref.wellhead_id in self._uncertain:
                return
        started = time.monotonic()
        try:
            result = self.registry.apply_control(
                ref.wellhead_id, lambda context: self.store.apply(ref, context)
            )
            self.metrics.control_outcomes.labels(outcome=result.reason).inc()
            logger.info(
                "Control command completed",
                extra={
                    "command_id": ref.command_id,
                    "wellhead_id": ref.wellhead_id,
                    "outcome": result.status,
                    "reason": result.reason,
                },
            )
        except (ControlCommitUnknownError, ValueError):
            with self._lock:
                self._uncertain.add(ref.wellhead_id)
            self.metrics.control_outcomes.labels(outcome="commit_unknown").inc()
            logger.error(
                "Control commit needs reconciliation",
                extra={"command_id": ref.command_id, "wellhead_id": ref.wellhead_id},
            )
        except DatabaseUnavailableError:
            self.metrics.control_outcomes.labels(outcome="retry").inc()
            logger.warning(
                "Control application will retry",
                extra={"command_id": ref.command_id, "wellhead_id": ref.wellhead_id},
            )
        finally:
            self.metrics.control_apply_duration.observe(time.monotonic() - started)
