"""Exercise the public launcher without starting or stopping a real stack."""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SECRET_KEYS = (
    "POSTGRES_PASSWORD",
    "TWIN_DB_PASSWORD",
    "API_DB_PASSWORD",
    "JWT_SECRET",
    "TWIN_INTERNAL_TOKEN",
    "PGADMIN_DEFAULT_PASSWORD",
    "GRAFANA_ADMIN_PASSWORD",
)


@dataclass
class Launcher:
    """Run a copied launcher with a Docker stub and an isolated configuration."""

    root: Path
    environment: dict[str, str]

    def run(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        """Invoke from another directory to exercise repository path handling."""
        return subprocess.run(  # noqa: S603
            [str(self.root / "simulator"), *arguments],
            cwd=self.root.parent,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

    def calls(self) -> list[list[str]]:
        """Read only recorded Docker arguments, never environment secrets."""
        log = self.root / "docker-calls.jsonl"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]


@pytest.fixture
def launcher(tmp_path: Path) -> Launcher:
    root = tmp_path / "demo with spaces"
    root.mkdir()
    for name in ("simulator", ".env.example"):
        shutil.copy2(ROOT / name, root / name)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "with Path(os.environ['DOCKER_CALL_LOG']).open('a') as out:\n"
        "    out.write(json.dumps(args) + '\\n')\n"
        "failure = os.environ.get('DOCKER_FAIL')\n"
        "if failure and failure in args:\n"
        "    sys.exit(1)\n"
        "if 'port' in args:\n"
        "    sys.stdout.write('127.0.0.1:18090\\n')\n"
    )
    docker.chmod(0o700)
    environment = dict(os.environ)
    environment.update(
        PATH=str(bin_dir) + os.pathsep + environment.get("PATH", ""),
        DOCKER_CALL_LOG=str(root / "docker-calls.jsonl"),
    )
    environment.pop("DOCKER_FAIL", None)
    return Launcher(root, environment)


def test_first_start_generates_private_distinct_secrets_and_waits(
    launcher: Launcher,
) -> None:
    result = launcher.run()
    assert result.returncode == 0, result.stderr
    configuration = launcher.root / ".env"
    assert stat.S_IMODE(configuration.stat().st_mode) == 0o600
    values = dict(
        line.split("=", 1)
        for line in configuration.read_text().splitlines()
        if line and not line.startswith("#")
    )
    secrets = [values[key] for key in SECRET_KEYS]
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in secrets)
    assert len(set(secrets)) == len(secrets)
    assert all(value not in result.stdout + result.stderr for value in secrets)
    assert not list(launcher.root.glob(".env.setup.*"))
    up = next(call for call in launcher.calls() if "up" in call)
    assert up[1:3] == ["--project-directory", str(launcher.root)]
    assert "--wait" in up and "--build" in up
    assert "Simulator ready: http://localhost:18090" in result.stdout
    assert "demo@example.com" in result.stdout


def test_repeated_start_preserves_configuration_without_executing_it(
    launcher: Launcher,
) -> None:
    configuration = launcher.root / ".env"
    marker = launcher.root / "must-not-exist"
    content = f"CUSTOM_VALUE=$(touch '{marker}')\nPOSTGRES_PASSWORD=local-test\n"
    configuration.write_text(content)
    for _ in range(2):
        assert launcher.run().returncode == 0
        assert configuration.read_text() == content
        assert not marker.exists()


@pytest.mark.parametrize("arguments", [("reset",), ("reset", "yes"), ("unknown",)])
def test_invalid_or_unconfirmed_actions_never_call_docker(
    launcher: Launcher, arguments: tuple[str, ...]
) -> None:
    assert launcher.run(*arguments).returncode == 2
    assert launcher.calls() == []
    assert not (launcher.root / ".env").exists()


def test_stop_keeps_data_and_confirmed_reset_reuses_configuration(
    launcher: Launcher,
) -> None:
    assert launcher.run().returncode == 0
    content = (launcher.root / ".env").read_bytes()
    assert launcher.run("stop").returncode == 0
    calls = launcher.calls()
    assert calls[-1][-3:] == ["--profile", "*", "stop"]
    assert not any("down" in call or "--volumes" in call for call in calls)
    assert launcher.run("reset", "--yes").returncode == 0
    calls = launcher.calls()
    down = next(index for index, call in enumerate(calls) if "down" in call)
    assert calls[down][-5:] == [
        "--profile",
        "*",
        "down",
        "--volumes",
        "--remove-orphans",
    ]
    assert any("up" in call for call in calls[down + 1 :])
    assert (launcher.root / ".env").read_bytes() == content


@pytest.mark.parametrize("failure", ["info", "version", "up"])
def test_docker_failures_never_report_success(launcher: Launcher, failure: str) -> None:
    launcher.environment["DOCKER_FAIL"] = failure
    result = launcher.run()
    assert result.returncode != 0
    assert "Simulator ready" not in result.stdout
    assert result.stderr
    assert (launcher.root / ".env").exists() == (failure == "up")


def test_failed_reset_does_not_start_the_stack(launcher: Launcher) -> None:
    (launcher.root / ".env").write_text("POSTGRES_PASSWORD=local-test\n")
    launcher.environment["DOCKER_FAIL"] = "down"
    result = launcher.run("reset", "--yes")
    assert result.returncode != 0
    assert not any("up" in call for call in launcher.calls())


def test_failed_random_source_leaves_no_partial_configuration(
    launcher: Launcher,
) -> None:
    fake_od = Path(launcher.environment["PATH"].split(os.pathsep)[0]) / "od"
    fake_od.write_text("#!/bin/sh\nexit 1\n")
    fake_od.chmod(0o700)
    result = launcher.run()
    assert result.returncode != 0
    assert not (launcher.root / ".env").exists()
    assert not list(launcher.root.glob(".env.setup.*"))
    assert not any("up" in call for call in launcher.calls())


def test_help_needs_no_docker(launcher: Launcher) -> None:
    assert launcher.run("--help").returncode == 0
    assert launcher.calls() == []
