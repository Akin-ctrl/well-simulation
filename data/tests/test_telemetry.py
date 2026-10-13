"""Tests for the telemetry pipeline's production-critical paths.

Covers the register codec, the freshness decision that prevents stale data from
being recorded as fresh, and the register store that addresses Modbus units.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import Mock

import pytest
from pymodbus.client.mixin import ModbusClientMixin

import database_ingestion as ingestion
import modbus_gateway as gateway
from telemetry_common import (
    HEARTBEAT_REGISTER,
    MODEL_VERSION_REGISTER,
    STATUS_BLOCK_REGISTERS,
    WORD_ORDER,
    JsonLogFormatter,
    ModelVersion,
    ParameterMapping,
    as_number,
    collect_unit_ids,
)

VERSION = ModelVersion(0, 3, 0)


def mapping(
    *,
    mapping_id: int = 1,
    wellhead_id: int = 1,
    parameter_type_id: int = 1,
    parameter_code: str = "tubing_pressure",
    modbus_register: int = 0,
    modbus_unit_id: int = 1,
    data_type: str = "float",
) -> ParameterMapping:
    """Build a ParameterMapping with sensible defaults for tests."""
    return ParameterMapping(
        mapping_id=mapping_id,
        wellhead_id=wellhead_id,
        parameter_type_id=parameter_type_id,
        parameter_code=parameter_code,
        modbus_register=modbus_register,
        modbus_unit_id=modbus_unit_id,
        data_type=data_type,
    )


def publish(store: gateway.RegisterStore, telemetry: list[dict[str, object]]) -> None:
    """Apply a batch the way the gateway does, stamped now with VERSION."""
    store.apply_batch(telemetry, datetime.now(timezone.utc), VERSION)


def status_registers(tick: int, version: list[int]) -> list[int]:
    """Build the five-register status block: heartbeat words, then version."""
    return (
        ModbusClientMixin.convert_to_registers(
            tick, ModbusClientMixin.DATATYPE.UINT32, word_order=WORD_ORDER
        )
        + version
    )


class TestRegisterCodec:
    """The gateway encodes and ingestion decodes; the two must agree exactly."""

    @pytest.mark.parametrize(
        ("data_type", "value"),
        [
            ("float", 2874.31),
            ("float", 0.0),
            ("float", 4999.99),
            ("integer", 100),
            ("integer", 0),
            ("boolean", 1),
            ("boolean", 0),
        ],
    )
    def test_round_trip(self, data_type: str, value: float) -> None:
        registers = gateway.encode_value(value, data_type)
        assert ingestion.decode_registers(registers, data_type) == pytest.approx(
            float(value), abs=0.01
        )

    def test_encode_uses_two_registers(self) -> None:
        assert len(gateway.encode_value(1.0, "float")) == ingestion.REGISTERS_PER_VALUE

    def test_encode_rejects_unknown_type(self) -> None:
        with pytest.raises(ValueError, match="Unsupported Modbus data type"):
            gateway.encode_value(1, "string")

    def test_decode_reports_unknown_type_without_raising(self) -> None:
        assert ingestion.decode_registers([0, 0], "string") is None

    def test_wire_format_is_stable(self) -> None:
        """Guards the on-disk register layout against an accidental change.

        These are the exact registers produced before the pymodbus 3.x upgrade.
        A change here invalidates every stored reading and every register map.
        """
        assert gateway.encode_value(1234.56, "float") == [20972, 17562]
        assert gateway.encode_value(100, "integer") == [100, 0]


class TestAsNumber:
    """Modbus decoding returns a union; only scalars are valid here."""

    def test_accepts_numbers(self) -> None:
        assert as_number(3, "x") == 3.0
        assert as_number(2.5, "x") == 2.5

    @pytest.mark.parametrize("value", ["12", [1, 2], None, True])
    def test_rejects_non_numeric(self, value: object) -> None:
        with pytest.raises(TypeError, match="Expected a numeric Modbus value"):
            as_number(value, "tubing_pressure")


class TestFreshness:
    """A stalled source still answers reads; recording those invents data."""

    def test_unchanged_heartbeat_is_stale(self) -> None:
        assert ingestion.is_stale(1000, 1000) is True

    def test_advanced_heartbeat_is_fresh(self) -> None:
        assert ingestion.is_stale(1005, 1000) is False

    def test_first_poll_is_fresh(self) -> None:
        assert ingestion.is_stale(1000, None) is False

    def test_unreadable_heartbeat_is_not_stale(self) -> None:
        """A transport failure is surfaced by the register reads, not here."""
        assert ingestion.is_stale(None, 1000) is True


class TestRegisterStore:
    """Register addressing must isolate values per Modbus unit."""

    def test_writes_value_to_mapped_unit_and_register(self) -> None:
        store = gateway.RegisterStore([mapping(modbus_register=10, modbus_unit_id=1)])
        publish(store, [{"wellhead_id": 1, "parameters": {"tubing_pressure": 1234.56}}])

        raw = store.context[1].getValues(3, 10, 2)
        assert ingestion.decode_registers(raw, "float") == pytest.approx(
            1234.56, abs=0.01
        )

    def test_units_do_not_share_an_address_space(self) -> None:
        store = gateway.RegisterStore(
            [
                mapping(wellhead_id=1, modbus_register=0, modbus_unit_id=1),
                mapping(wellhead_id=2, modbus_register=0, modbus_unit_id=7),
            ]
        )
        publish(
            store,
            [
                {"wellhead_id": 1, "parameters": {"tubing_pressure": 111.0}},
                {"wellhead_id": 2, "parameters": {"tubing_pressure": 222.0}},
            ],
        )

        assert ingestion.decode_registers(
            store.context[1].getValues(3, 0, 2), "float"
        ) == pytest.approx(111.0)
        assert ingestion.decode_registers(
            store.context[7].getValues(3, 0, 2), "float"
        ) == pytest.approx(222.0)

    def test_heartbeat_written_for_every_unit(self) -> None:
        store = gateway.RegisterStore(
            [
                mapping(wellhead_id=1, modbus_unit_id=1),
                mapping(wellhead_id=2, modbus_unit_id=7),
            ]
        )
        before = datetime.now(timezone.utc).timestamp()
        publish(store, [{"wellhead_id": 1, "parameters": {}}])

        for unit_id in (1, 7):
            raw = store.context[unit_id].getValues(3, HEARTBEAT_REGISTER, 2)
            written = as_number(
                ModbusClientMixin.convert_from_registers(
                    raw, ModbusClientMixin.DATATYPE.UINT32, word_order="little"
                ),
                "heartbeat",
            )
            assert written >= before - 1
            assert store.context[unit_id].getValues(3, MODEL_VERSION_REGISTER, 3) == [
                0,
                3,
                0,
            ]

    def test_clear_unpublishes_heartbeat_and_version(self) -> None:
        """Mid-update, the whole status block reads zero, not only the tick."""
        store = gateway.RegisterStore([mapping()])
        publish(store, [{"wellhead_id": 1, "parameters": {}}])
        store.clear_heartbeat()
        assert (
            store.context[1].getValues(3, HEARTBEAT_REGISTER, STATUS_BLOCK_REGISTERS)
            == [0] * STATUS_BLOCK_REGISTERS
        )

    def test_status_block_round_trips_from_gateway_to_ingestion(self) -> None:
        """What the gateway writes is exactly what ingestion reads back."""
        store = gateway.RegisterStore([mapping()])
        at = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
        store.apply_batch([{"wellhead_id": 1, "parameters": {}}], at, VERSION)

        def read(address: int, *, count: int, device_id: int) -> Mock:
            result = Mock(
                registers=store.context[device_id].getValues(3, address, count)
            )
            result.isError.return_value = False
            return result

        client = Mock()
        client.read_holding_registers.side_effect = read
        assert ingestion.read_status(client, 1) == ingestion.GatewayStatus(
            int(at.timestamp()), VERSION
        )

    def test_unknown_wellhead_is_skipped_not_fatal(self) -> None:
        store = gateway.RegisterStore([mapping(wellhead_id=1)])
        publish(store, [{"wellhead_id": 99, "parameters": {"tubing_pressure": 1.0}}])

    def test_malformed_batch_is_rejected(self) -> None:
        store = gateway.RegisterStore([mapping()])
        with pytest.raises(TypeError, match="Malformed telemetry batch"):
            publish(store, [{"wellhead_id": "one", "parameters": {}}])

    def test_heartbeat_clear_of_mapped_registers(self) -> None:
        """12 wellheads x 18 parameters tops out at register 1134."""
        highest_mapped = (12 - 1) * 100 + (18 - 1) * 2
        assert highest_mapped < HEARTBEAT_REGISTER
        # The whole status block must fit inside the writable register range.
        assert HEARTBEAT_REGISTER + STATUS_BLOCK_REGISTERS <= gateway.USABLE_REGISTERS


class TestUnitIds:
    def test_collects_sorted_and_deduplicated(self) -> None:
        mappings = [
            mapping(modbus_unit_id=7),
            mapping(modbus_unit_id=1),
            mapping(modbus_unit_id=7),
        ]
        assert collect_unit_ids(mappings) == [1, 7]


@pytest.mark.parametrize(
    ("second_tick", "second_version", "accepted"),
    [
        (1_800_000_000, [0, 3, 0], True),
        (1_800_000_001, [0, 3, 0], False),
        (0, [0, 3, 0], False),
        (1_800_000_000, [0, 4, 0], False),
        (1_800_000_000, [0, 0, 0], False),
    ],
)
def test_fleet_status_requires_every_unit_on_one_tick_and_version(
    second_tick: int, second_version: list[int], accepted: bool
) -> None:
    """A mixed, unready, or unversioned Modbus unit must not create a batch."""
    first_tick = 1_800_000_000
    client = Mock()

    def read_result(address: int, *, count: int, device_id: int) -> Mock:
        # One request per unit covers the heartbeat and the version.
        assert address == HEARTBEAT_REGISTER
        assert count == STATUS_BLOCK_REGISTERS
        registers = (
            status_registers(first_tick, [0, 3, 0])
            if device_id == 1
            else status_registers(second_tick, second_version)
        )
        result = Mock(registers=registers)
        result.isError.return_value = False
        return result

    client.read_holding_registers.side_effect = read_result
    mappings = [
        mapping(modbus_unit_id=1),
        mapping(mapping_id=2, wellhead_id=2, modbus_unit_id=7),
    ]
    status = ingestion.read_fleet_status(client, mappings)
    expected = ingestion.GatewayStatus(first_tick, VERSION)
    assert (status == expected) is accepted
    assert client.read_holding_registers.call_count == 2


class TestReadingRow:
    """The insert tuple must stay aligned with the SQL column order."""

    def test_row_order_matches_insert_columns(self) -> None:
        now = datetime.now(timezone.utc)
        reading = ingestion.Reading(
            timestamp_utc=now,
            wellhead_id=3,
            parameter_type_id=4,
            mapping_id=5,
            raw_value=6.5,
            model_version=VERSION,
        )
        assert reading.as_row() == (
            now,
            3,
            4,
            5,
            6.5,
            "synthetic_reduced_order_model",
            "0.3.0",
        )

        columns = (
            ingestion.INSERT_READING_SQL.split("(", 1)[1].split(")", 1)[0].split(",")
        )
        assert [c.strip() for c in columns] == [
            "timestamp_utc",
            "wellhead_id",
            "parameter_type_id",
            "mapping_id",
            "raw_value",
            "source_kind",
            "model_version",
        ]


class TestJsonLogFormatter:
    """Standard 2: one parser must be able to read every service's logs."""

    def _record(self, **extra: object) -> logging.LogRecord:
        record = logging.LogRecord(
            "test", logging.INFO, __file__, 1, "hello %s", ("world",), None
        )
        record.__dict__.update(extra)
        return record

    def test_emits_valid_json_with_expected_fields(self) -> None:
        payload = json.loads(JsonLogFormatter("svc").format(self._record()))
        assert payload["service"] == "svc"
        assert payload["level"] == "info"
        assert payload["message"] == "hello world"
        assert "timestamp" in payload

    def test_extras_become_queryable_top_level_fields(self) -> None:
        payload = json.loads(
            JsonLogFormatter("svc").format(self._record(wellhead_id=7, count=216))
        )
        assert payload["wellhead_id"] == 7
        assert payload["count"] == 216

    def test_exceptions_are_captured(self) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            record = logging.LogRecord(
                "test", logging.ERROR, __file__, 1, "failed", None, sys.exc_info()
            )
        payload = json.loads(JsonLogFormatter("svc").format(record))
        assert "ValueError: boom" in payload["error"]


def test_invalid_modbus_value_rejects_the_entire_poll() -> None:
    client = Mock()
    client.read_holding_registers.return_value = Mock(
        registers=gateway.encode_value(float("nan"), "float")
    )
    client.read_holding_registers.return_value.isError.return_value = False
    with pytest.raises(ValueError, match="Nonfinite"):
        ingestion.poll_once(client, [mapping()], datetime.now(timezone.utc), VERSION)


def test_model_heartbeat_age_is_bounded() -> None:
    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    epoch = int(now.timestamp())
    assert ingestion.current_heartbeat(epoch - 10, now, 5) == epoch - 10
    assert ingestion.current_heartbeat(epoch - 11, now, 5) is None
    assert ingestion.current_heartbeat(epoch + 3, now, 5) is None


class TestModelVersion:
    """One definition of the version's text and register forms."""

    def test_text_and_registers_round_trip(self) -> None:
        version = ModelVersion.parse("12.0.65535")
        assert version.to_registers() == [12, 0, 65535]
        assert ModelVersion.from_registers(version.to_registers()) == version
        assert str(version) == "12.0.65535"

    def test_zero_block_means_unpublished(self) -> None:
        assert ModelVersion.from_registers([0, 0, 0]) is None

    @pytest.mark.parametrize(
        "text", ["", "0.3", "0.3.0.1", "v0.3.0", "0.3.x", "0.3.70000", "0.0.0", "٣.1.1"]
    )
    def test_rejects_text_that_cannot_be_three_registers(self, text: str) -> None:
        with pytest.raises(ValueError, match="Model version"):
            ModelVersion.parse(text)

    def test_rejects_non_text(self) -> None:
        with pytest.raises(ValueError, match="must be text"):
            ModelVersion.parse(3)


class TestPollRetry:
    """A gateway update during a poll is retried, never stored as a mixed batch."""

    MAPPINGS: ClassVar[list[ParameterMapping]] = [
        mapping(mapping_id=1, parameter_code="tubing_pressure", modbus_register=0),
        mapping(
            mapping_id=2,
            parameter_type_id=2,
            parameter_code="flow_rate",
            modbus_register=2,
        ),
    ]

    def _publish(
        self, store: gateway.RegisterStore, offset_s: int, value: float
    ) -> None:
        """Publish one tick `offset_s` seconds from now with both values set."""
        at = datetime.fromtimestamp(
            int(datetime.now(timezone.utc).timestamp()) + offset_s, timezone.utc
        )
        store.apply_batch(
            [
                {
                    "wellhead_id": 1,
                    "parameters": {"tubing_pressure": value, "flow_rate": value},
                }
            ],
            at,
            VERSION,
        )

    def _client(
        self,
        store: gateway.RegisterStore,
        on_value_read: Callable[[], None] | None = None,
    ) -> Mock:
        """Build a Modbus client that reads the real register image.

        `on_value_read` runs after each value register is read, which is where
        a test makes the gateway publish mid-poll.
        """

        def read(address: int, *, count: int, device_id: int) -> Mock:
            result = Mock(
                registers=store.context[device_id].getValues(3, address, count)
            )
            result.isError.return_value = False
            if address != HEARTBEAT_REGISTER and on_value_read is not None:
                on_value_read()
            return result

        client = Mock()
        client.read_holding_registers.side_effect = read
        return client

    def _start(self, client: Mock) -> ingestion.GatewayStatus:
        status = ingestion.read_fleet_status(client, self.MAPPINGS)
        assert status is not None
        return status

    def test_clean_poll_needs_no_retry(self) -> None:
        store = gateway.RegisterStore(self.MAPPINGS)
        self._publish(store, -1, 100.0)
        client = self._client(store)
        status = self._start(client)

        result = ingestion.poll_consistent_batch(
            client, self.MAPPINGS, status, time.monotonic() + 2.5, 5
        )

        assert result is not None
        polled, readings = result
        assert polled == status
        assert [r.raw_value for r in readings] == [100.0, 100.0]

    def test_clean_poll_finishing_after_deadline_is_discarded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A coherent but late batch must not consume the next poll interval."""
        store = gateway.RegisterStore(self.MAPPINGS)
        self._publish(store, -1, 100.0)
        elapsed = [100.0]
        value_reads = [0]

        def slow_read() -> None:
            value_reads[0] += 1
            elapsed[0] = 101.0

        client = self._client(store, slow_read)
        status = self._start(client)
        monkeypatch.setattr(
            ingestion, "time", SimpleNamespace(monotonic=lambda: elapsed[0])
        )

        result = ingestion.poll_consistent_batch(
            client, self.MAPPINGS, status, deadline=100.5, interval_seconds=5
        )

        assert value_reads[0] == len(self.MAPPINGS)
        assert result is None

    def test_update_mid_poll_is_retried_on_the_new_tick(self) -> None:
        store = gateway.RegisterStore(self.MAPPINGS)
        self._publish(store, -1, 100.0)
        published: list[bool] = []

        def publish_once() -> None:
            if not published:
                published.append(True)
                self._publish(store, 0, 200.0)

        client = self._client(store, publish_once)
        status = self._start(client)

        result = ingestion.poll_consistent_batch(
            client, self.MAPPINGS, status, time.monotonic() + 2.5, 5
        )

        assert result is not None
        polled, readings = result
        assert polled.heartbeat == status.heartbeat + 1
        # Every reading comes from the new tick: no mix of 100 and 200.
        assert [r.raw_value for r in readings] == [200.0, 200.0]
        assert {r.timestamp_utc for r in readings} == {
            datetime.fromtimestamp(polled.heartbeat, timezone.utc)
        }

    def test_gives_up_at_the_deadline_when_every_poll_is_overtaken(self) -> None:
        store = gateway.RegisterStore(self.MAPPINGS)
        self._publish(store, -3, 100.0)
        ticks = iter(range(-2, 60))

        def publish_every_time() -> None:
            self._publish(store, next(ticks), 300.0)

        client = self._client(store, publish_every_time)
        status = self._start(client)
        started = time.monotonic()

        result = ingestion.poll_consistent_batch(
            client, self.MAPPINGS, status, started + 0.35, 5
        )

        assert result is None
        assert time.monotonic() - started < 1.0

    def test_gives_up_when_the_update_never_completes(self) -> None:
        store = gateway.RegisterStore(self.MAPPINGS)
        self._publish(store, -1, 100.0)
        client = self._client(store, store.clear_heartbeat)
        status = self._start(client)

        result = ingestion.poll_consistent_batch(
            client, self.MAPPINGS, status, time.monotonic() + 2.5, 5
        )

        assert result is None
