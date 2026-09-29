"""Start the twin-core service boundary and stop it cleanly."""

from __future__ import annotations

import logging
import signal
import threading

from twin_core.config import (
    NoDatabaseOverrides,
    ServiceSettings,
    load_defaults,
    resolve_parameters,
)
from twin_core.database import HistorianReader
from twin_core.http_api import create_server
from twin_core.logging_config import configure_logging
from twin_core.metrics import TwinMetrics
from twin_core.runtime import TwinRuntime
from twin_core.state import ModelRegistry

logger = logging.getLogger(__name__)


def main() -> int:
    """Run HTTP and the fixed-step loop in one container."""
    configure_logging()
    try:
        settings = ServiceSettings.from_env()
        defaults = load_defaults()
        # Reject invalid global values even when the database has no wells.
        resolve_parameters(defaults, settings.environment_overrides, {})
        registry = ModelRegistry(
            defaults, settings.environment_overrides, NoDatabaseOverrides()
        )
        metrics = TwinMetrics()
        runtime = TwinRuntime(
            settings, defaults, registry, HistorianReader(settings), metrics
        )
        server = create_server(settings.bind_host, settings.port, runtime)
    except (OSError, ValueError):
        logger.error("Twin-core startup configuration is invalid")
        return 1

    def request_stop(_signum: int, _frame: object) -> None:
        """Ask the loop to finish without interrupting a database read."""
        runtime.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    http_thread = threading.Thread(target=server.serve_forever, daemon=True)
    http_thread.start()
    logger.info("Twin-core service started")
    try:
        runtime.run()
    finally:
        server.shutdown()
        server.server_close()
        http_thread.join(timeout=5)
        logger.info("Twin-core service stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
