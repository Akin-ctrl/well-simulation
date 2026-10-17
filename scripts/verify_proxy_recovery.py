"""Verify shipped nginx recovery on an isolated Docker network, without a database.

Build the service images first, then run this script. It uses the dashboard
image's real configuration and health command. A small HTTP fixture runs in the
API image so Docker DNS, changed addresses, and proxy behavior remain real.
Only containers and a network created by this invocation are removed.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger("proxy_recovery")


class ProxyCheck:
    """Own temporary containers and assertions about the actual proxy image."""

    def __init__(self, dashboard_image: str, api_image: str) -> None:
        """Allocate unique names without touching a running Compose project."""
        executable = shutil.which("docker")
        if executable is None:
            raise RuntimeError("Docker is required")
        self.executable = executable
        self.dashboard_image = dashboard_image
        self.api_image = api_image
        self.prefix = "well-proxy-check-" + uuid4().hex[:12]
        self.network = self.prefix + "-network"
        self.dashboard = self.prefix + "-dashboard"
        self.api = self.prefix + "-api"
        self.blocker = self.prefix + "-address-holder"
        self.containers: list[str] = []
        self.network_created = False
        self.base = ""

    def docker(self, *arguments: str, check: bool = True) -> str:
        """Run fixed Docker operations and report failures without environment data."""
        result = subprocess.run(  # noqa: S603
            [self.executable, *arguments], capture_output=True, text=True, timeout=60
        )
        if check and result.returncode:
            raise RuntimeError(result.stderr.strip() or "Docker operation failed")
        return result.stdout.strip()

    def inspect(self, name: str, template: str) -> str:
        """Read one chosen field, never container credentials."""
        return self.docker("inspect", "--format", template, name)

    def request(self, path: str, *, body: bytes | None = None) -> tuple[int, str]:
        """Send one request, preserving error responses for explicit assertions."""
        request = Request(  # noqa: S310
            self.base + path,
            data=body,
            headers={"Origin": self.base, "Content-Type": "application/json"},
        )
        try:
            response = urlopen(request, timeout=5)  # noqa: S310
        except HTTPError as error:
            response = error
        with response:
            return response.status, response.read().decode()

    def wait_http(self, path: str, status: int) -> None:
        """Bound HTTP recovery to thirty seconds, including DNS refresh."""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if self.request(path)[0] == status:
                    return
            except (URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(0.5)
        raise RuntimeError(f"{path} did not return {status} before the deadline")

    def wait_health(self, expected: str) -> None:
        """Wait for Docker to run the image's own readiness command."""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.inspect(self.dashboard, "{{.State.Health.Status}}") == expected:
                return
            time.sleep(0.5)
        raise RuntimeError("Dashboard health did not become " + expected)

    def start_api(self) -> None:
        """Start a fixture under the same DNS service name used by Compose."""
        self.containers.append(self.api)
        self.docker(
            "run",
            "--detach",
            "--name",
            self.api,
            "--network",
            self.network,
            "--network-alias",
            "api",
            "--entrypoint",
            "node",
            "--mount",
            f"type=bind,src={ROOT / 'scripts/fixtures/proxy-api.mjs'},"
            "dst=/proxy-api.mjs,readonly",
            self.api_image,
            "/proxy-api.mjs",
        )

    def start_dashboard(self) -> None:
        """Publish only a random loopback port and accelerate health scheduling."""
        self.docker("network", "create", self.network)
        self.network_created = True
        self.containers.append(self.dashboard)
        self.docker(
            "run",
            "--detach",
            "--name",
            self.dashboard,
            "--network",
            self.network,
            "--publish",
            "127.0.0.1::80",
            "--health-interval",
            "2s",
            "--health-timeout",
            "5s",
            "--health-retries",
            "2",
            "--health-start-period",
            "1s",
            self.dashboard_image,
        )
        port = self.inspect(
            self.dashboard,
            '{{(index (index .NetworkSettings.Ports "80/tcp") 0).HostPort}}',
        )
        self.base = "http://127.0.0.1:" + port

    def verify_requests(self) -> None:
        """Require prefix removal, query/body forwarding, and command non-replay."""
        status, body = self.request(
            "/api/echo?wellheadId=1&name=a%20b", body=b'{"x":1}'
        )
        payload = json.loads(body)
        if status != 200 or payload != {
            "url": "/echo?wellheadId=1&name=a%20b",
            "method": "POST",
            "host": self.base.removeprefix("http://"),
            "origin": self.base,
            "body": '{"x":1}',
        }:
            raise RuntimeError("Proxy changed a request's route, headers, or body")
        if self.request("/api/fixture/command", body=b"{}")[0] != 502:
            raise RuntimeError("A lost command response did not remain an error")
        if json.loads(self.request("/api/fixture/commands")[1]) != {"commands": 1}:
            raise RuntimeError("Proxy replayed a command")

    def verify_replacement(self) -> None:
        """Remove the API, occupy its address, and require automatic recovery."""
        address = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
        old_ip = self.inspect(self.api, address)
        started = self.inspect(self.dashboard, "{{.State.StartedAt}}")
        self.docker("rm", "--force", self.api)
        self.wait_health("unhealthy")
        if self.request("/")[0] != 200 or self.request("/api/ready")[0] != 502:
            raise RuntimeError("Missing API did not leave a visible proxy failure")
        self.containers.append(self.blocker)
        self.docker(
            "run",
            "--detach",
            "--name",
            self.blocker,
            "--network",
            self.network,
            "--entrypoint",
            "node",
            self.api_image,
            "-e",
            "setTimeout(() => {}, 600000)",
        )
        self.start_api()
        new_ip = self.inspect(self.api, address)
        if new_ip == old_ip:
            raise RuntimeError("Fixture did not exercise an actual IP address change")
        began = time.monotonic()
        self.wait_http("/api/ready", 200)
        self.wait_health("healthy")
        if self.inspect(self.dashboard, "{{.State.StartedAt}}") != started:
            raise RuntimeError("Dashboard restarted during the recovery check")
        LOGGER.info(
            "API address changed %s -> %s; recovered in %.1f seconds",
            old_ip,
            new_ip,
            time.monotonic() - began,
        )

    def run(self) -> None:
        """Check absent startup, availability, dependency failure, and replacement."""
        try:
            self.start_dashboard()
            self.wait_http("/", 200)
            self.wait_health("unhealthy")
            LOGGER.info("Dashboard starts without API DNS; readiness reports unhealthy")
            self.start_api()
            self.wait_http("/api/ready", 200)
            self.wait_health("healthy")
            self.verify_requests()
            LOGGER.info("Late API startup and HTTP forwarding passed")
            self.request("/api/fixture/readiness?ready=false")
            self.wait_http("/api/ready", 503)
            self.wait_health("unhealthy")
            self.request("/api/fixture/readiness?ready=true")
            self.wait_health("healthy")
            LOGGER.info("API readiness failure and recovery passed")
            self.verify_replacement()
            self.verify_requests()
            LOGGER.info(
                "Proxy startup, readiness, forwarding, and recovery checks passed"
            )
        finally:
            for name in reversed(list(dict.fromkeys(self.containers))):
                self.docker("rm", "--force", name, check=False)
            if self.network_created:
                self.docker("network", "rm", self.network, check=False)


def main() -> None:
    """Read image names and run the isolated operational regression check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dashboard-image", default="well-simulation-dashboard")
    parser.add_argument("--api-image", default="well-simulation-api")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ProxyCheck(args.dashboard_image, args.api_image).run()


if __name__ == "__main__":
    main()
