# ADR 0001: Position The Project As A Wellhead Monitoring Simulation Before Digital Twin

- Status: Accepted
- Date: 2026-05-31

## Context

The project currently simulates wellhead telemetry, exposes it through Modbus TCP, stores readings in TimescaleDB, evaluates alarm rules, and has a dashboard/API foundation.

It does not yet include a stateful process model, prediction, calibration, or what-if simulation. It is on the road to becoming a digital twin.
