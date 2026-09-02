"""Tests for the metadata reload from ADR 0035.

The services configure themselves from database tables but used to read them
once at startup, so an edited row did nothing until the container restarted.
These cover the timer, the change detection, and the register clearing.
"""

from __future__ import annotations

import psycopg2
import pytest
from pytest import MonkeyPatch

import modbus_gateway as gateway
from telemetry_common import (
    HEARTBEAT_REGISTER,
    DatabaseSettings,
    MetadataReloader,
    ParameterMapping,
    mappings_changed,
)

# Never used to connect: every test that touches the loader replaces it.
SETTINGS = DatabaseSettings(
    host="localhost",
    port=5432,
    name="x",
    user="x",
    password="unused",  # noqa: S106
)


def mapping(**overrides: object) -> ParameterMapping:
    """Build a ParameterMapping with defaults, overriding named fields."""
    fields: dict[str, object] = {
        "mapping_id": 1,
        "wellhead_id": 1,
        "parameter_type_id": 1,
        "parameter_code": "tubing_pressure",
        "modbus_register": 0,
        "modbus_unit_id": 1,
        "data_type": "float",
    }
    fields.update(overrides)
    return ParameterMapping(**fields)  # type: ignore[arg-type]


class FakeClock:
    """A clock the test advances by hand."""

    def __init__(self) -> None:
        """Start the clock at zero."""
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestReloadTimer:
    def test_not_due_before_the_interval_elapses(self) -> None:
        clock = FakeClock()
        reloader = MetadataReloader(SETTINGS, 5, clock=clock)

        clock.advance(4.9)
        assert reloader.is_due() is False

    def test_due_once_the_interval_elapses(self) -> None:
        clock = FakeClock()
        reloader = MetadataReloader(SETTINGS, 5, clock=clock)

        clock.advance(5)
        assert reloader.is_due() is True

    def test_a_failed_read_still_resets_the_timer(
        self, monkeypatch: MonkeyPatch
    ) -> None:
        """A database that is down must not turn the reload into a hot loop."""
        clock = FakeClock()
        reloader = MetadataReloader(SETTINGS, 5, clock=clock)
        clock.advance(5)

        def fail(_: DatabaseSettings) -> list[ParameterMapping]:
            raise psycopg2.OperationalError("down")

        monkeypatch.setattr("telemetry_common.load_parameter_mappings", fail)

        assert reloader.reload() is None
        assert reloader.is_due() is False

    def test_a_failed_read_returns_none_rather_than_raising(
        self, monkeypatch: MonkeyPatch
    ) -> None:
        """The caller keeps the mappings it already has."""
        reloader = MetadataReloader(SETTINGS, 0)

        def fail(_: DatabaseSettings) -> list[ParameterMapping]:
            raise psycopg2.OperationalError("down")

        monkeypatch.setattr("telemetry_common.load_parameter_mappings", fail)
        assert reloader.reload() is None


class TestChangeDetection:
    def test_identical_sets_are_not_a_change(self) -> None:
        before = [mapping(mapping_id=1), mapping(mapping_id=2)]
        assert mappings_changed(before, list(before)) is False

    def test_reordering_is_not_a_change(self) -> None:
        before = [mapping(mapping_id=1), mapping(mapping_id=2)]
        assert mappings_changed(before, list(reversed(before))) is False

    def test_a_moved_register_is_a_change(self) -> None:
        before = [mapping(modbus_register=0)]
        after = [mapping(modbus_register=200)]
        assert mappings_changed(before, after) is True

    def test_a_removed_mapping_is_a_change(self) -> None:
        before = [mapping(mapping_id=1), mapping(mapping_id=2)]
        assert mappings_changed(before, [before[0]]) is True


class TestRegisterClearing:
    def test_a_moved_mapping_leaves_no_value_behind(self) -> None:
        """ADR 0035: the old address must not keep serving a stale reading."""
        store = gateway.RegisterStore([mapping(modbus_register=0)])
        store.apply_batch(
            [{"wellhead_id": 1, "parameters": {"tubing_pressure": 1234.5}}]
        )
        assert store.context[1].getValues(3, 0, 2) != [0, 0]

        store.replace_mappings([mapping(modbus_register=200)])

        assert store.context[1].getValues(3, 0, 2) == [0, 0]

    def test_the_heartbeat_is_cleared_too(self) -> None:
        """A surviving heartbeat would tell ingestion the data is fresh."""
        store = gateway.RegisterStore([mapping()])
        store.apply_batch([{"wellhead_id": 1, "parameters": {}}])
        assert store.context[1].getValues(3, HEARTBEAT_REGISTER, 2) != [0, 0]

        store.replace_mappings([mapping()])

        assert store.context[1].getValues(3, HEARTBEAT_REGISTER, 2) == [0, 0]

    def test_the_new_mappings_are_addressable(self) -> None:
        store = gateway.RegisterStore([mapping(modbus_register=0)])
        store.replace_mappings([mapping(modbus_register=200)])

        store.apply_batch([{"wellhead_id": 1, "parameters": {"tubing_pressure": 42.0}}])

        assert store.context[1].getValues(3, 200, 2) != [0, 0]
        assert store.context[1].getValues(3, 0, 2) == [0, 0]

    def test_a_new_unit_id_gets_its_own_context(self) -> None:
        store = gateway.RegisterStore([mapping(modbus_unit_id=1)])
        store.replace_mappings(
            [mapping(modbus_unit_id=1), mapping(wellhead_id=2, modbus_unit_id=7)]
        )
        assert store.unit_ids == [1, 7]


class TestApplyMetadataReload:
    def test_returns_the_current_mappings_when_not_due(
        self, monkeypatch: MonkeyPatch
    ) -> None:
        clock = FakeClock()
        reloader = MetadataReloader(SETTINGS, 5, clock=clock)
        store = gateway.RegisterStore([mapping()])
        current = [mapping()]

        def explode(_: DatabaseSettings) -> list[ParameterMapping]:
            raise AssertionError("must not read the database before it is due")

        monkeypatch.setattr("telemetry_common.load_parameter_mappings", explode)
        assert gateway.apply_metadata_reload(store, reloader, current) is current

    def test_adopts_a_changed_set(self, monkeypatch: MonkeyPatch) -> None:
        clock = FakeClock()
        reloader = MetadataReloader(SETTINGS, 5, clock=clock)
        clock.advance(5)
        store = gateway.RegisterStore([mapping(modbus_register=0)])
        moved = [mapping(modbus_register=200)]

        monkeypatch.setattr("telemetry_common.load_parameter_mappings", lambda _: moved)
        result = gateway.apply_metadata_reload(store, reloader, [mapping()])

        assert result == moved
        assert store.context[1].getValues(3, 0, 2) == [0, 0]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
