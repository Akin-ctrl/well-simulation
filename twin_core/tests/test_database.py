"""Tests for telemetry timestamps, freshness, and missing values."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from twin_core.database import make_snapshot

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def test_fresh_complete_readings_keep_both_timestamps() -> None:
    source = NOW - timedelta(seconds=4)
    inserted = NOW - timedelta(seconds=2)
    snapshot = make_snapshot(
        1,
        [("tubing_pressure", "psi", 1800.0, source, inserted)],
        NOW,
        15,
    )
    payload = snapshot.to_payload()
    assert payload["quality"] == "fresh"
    assert payload["complete"] is True
    assert payload["modelGenerated"] is False
    reading = snapshot.readings[0]
    assert reading.source_timestamp == source
    assert reading.inserted_at == inserted


def test_partial_readings_keep_missing_as_null() -> None:
    snapshot = make_snapshot(
        1,
        [
            ("flow_rate", "bbl/day", 100.0, NOW, NOW),
            ("water_cut", "%", None, None, None),
        ],
        NOW,
        15,
    )
    assert snapshot.quality == "partial"
    assert snapshot.complete is False
    assert snapshot.readings[1].value is None


def test_stale_future_and_invalid_values_are_visible() -> None:
    snapshot = make_snapshot(
        1,
        [
            ("old", "psi", 100.0, NOW - timedelta(seconds=20), NOW),
            ("future", "psi", 100.0, NOW + timedelta(seconds=10), NOW),
            ("bad", "psi", float("nan"), NOW, NOW),
        ],
        NOW,
        15,
    )
    assert [reading.quality for reading in snapshot.readings] == [
        "stale",
        "clock_skew",
        "invalid",
    ]
    assert snapshot.quality == "invalid"
    assert snapshot.readings[2].value is None
