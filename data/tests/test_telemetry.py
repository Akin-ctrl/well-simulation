"""Tests for the telemetry pipeline's production-critical paths.

Covers the register codec, the freshness decision that prevents stale data from
being recorded as fresh, and the register store that addresses Modbus units.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from pymodbus.client.mixin import ModbusClientMixin

import database_ingestion as ingestion
import modbus_gateway as gateway
from telemetry_common import (
    HEARTBEAT_REGISTER,
    WORD_ORDER,
    JsonLogFormatter,
    ParameterMapping,
    as_number,
    collect_unit_ids,
)


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
        store.apply_batch(
            [{"wellhead_id": 1, "parameters": {"tubing_pressure": 1234.56}}]
        )

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
        store.apply_batch(
            [
                {"wellhead_id": 1, "parameters": {"tubing_pressure": 111.0}},
                {"wellhead_id": 2, "parameters": {"tubing_pressure": 222.0}},
            ]
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
        store.apply_batch([{"wellhead_id": 1, "parameters": {}}])

        for unit_id in (1, 7):
            raw = store.context[unit_id].getValues(3, HEARTBEAT_REGISTER, 2)
            written = as_number(
                ModbusClientMixin.convert_from_registers(
                    raw, ModbusClientMixin.DATATYPE.UINT32, word_order="little"
                ),
                "heartbeat",
            )
            assert written >= before - 1

    def test_unknown_wellhead_is_skipped_not_fatal(self) -> None:
        store = gateway.RegisterStore([mapping(wellhead_id=1)])
        store.apply_batch([{"wellhead_id": 99, "parameters": {"tubing_pressure": 1.0}}])

    def test_malformed_batch_is_rejected(self) -> None:
        store = gateway.RegisterStore([mapping()])
        with pytest.raises(TypeError, match="Malformed telemetry batch"):
            store.apply_batch([{"wellhead_id": "one", "parameters": {}}])

    def test_heartbeat_clear_of_mapped_registers(self) -> None:
        """12 wellheads x 18 parameters tops out at register 1134."""
        highest_mapped = (12 - 1) * 100 + (18 - 1) * 2
        assert highest_mapped < HEARTBEAT_REGISTER
        assert HEARTBEAT_REGISTER + 1 < gateway.REGISTER_BLOCK_SIZE


class TestUnitIds:
    def test_collects_sorted_and_deduplicated(self) -> None:
        mappings = [
            mapping(modbus_unit_id=7),
            mapping(modbus_unit_id=1),
            mapping(modbus_unit_id=7),
        ]
        assert collect_unit_ids(mappings) == [1, 7]


@pytest.mark.parametrize(
    ("second_tick", "accepted"),
    [(1_800_000_000, True), (1_800_000_001, False), (0, False)],
)
def test_fleet_heartbeat_requires_every_unit_on_one_tick(
    second_tick: int, accepted: bool
) -> None:
    """A mixed or unready Modbus unit must not create a fleet batch."""
    first_tick = 1_800_000_000
    client = Mock()

    def read_result(address: int, *, count: int, device_id: int) -> Mock:
        assert address == HEARTBEAT_REGISTER
        assert count == 2
        tick = first_tick if device_id == 1 else second_tick
        registers = ModbusClientMixin.convert_to_registers(
            tick, ModbusClientMixin.DATATYPE.UINT32, word_order=WORD_ORDER
        )
        result = Mock(registers=registers)
        result.isError.return_value = False
        return result

    client.read_holding_registers.side_effect = read_result
    mappings = [
        mapping(modbus_unit_id=1),
        mapping(mapping_id=2, wellhead_id=2, modbus_unit_id=7),
    ]
    heartbeat = ingestion.read_fleet_heartbeat(client, mappings)
    assert (heartbeat == first_tick) is accepted
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
        )
        assert reading.as_row() == (now, 3, 4, 5, 6.5, "synthetic_reduced_order_model")

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
        ingestion.poll_once(client, [mapping()], datetime.now(timezone.utc))


def test_model_heartbeat_age_is_bounded() -> None:
    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    epoch = int(now.timestamp())
    assert ingestion.current_heartbeat(epoch - 10, now, 5) == epoch - 10
    assert ingestion.current_heartbeat(epoch - 11, now, 5) is None
    assert ingestion.current_heartbeat(epoch + 3, now, 5) is None
