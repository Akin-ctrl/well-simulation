# ADR 0001: Position The Project As A Wellhead Monitoring Simulation Before Digital Twin

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Implemented

## Context

The project currently simulates wellhead telemetry, exposes it through Modbus TCP, stores readings in TimescaleDB, evaluates alarm rules, and has a dashboard/API foundation.

It does not yet include a stateful process model, prediction, calibration, or what-if simulation. It is on the road to becoming a digital twin.

## Amendment 2026-10-17: Describe the implemented synthetic system

The context above describes the original project. Twin-core now calculates all
18 seeded signals, saves state for recovery, and runs forecasts and isolated
scenarios. The dashboard includes historical branches, scheduled forecasts,
reviewed synthetic controls, parameter publication and alarm operator actions.

Keep the wellhead monitoring simulation name. The implemented model has no field
calibration or physical control connection. A completed software workflow does
not establish accuracy for a real well. The
[architecture overview](../architecture/overview.md) records the current system.

**Good.** Readers can distinguish completed model features from field validation.

**Costs.** Capability descriptions need updates when another workflow changes.

**Accepted risks.** Synthetic results can look plausible without being accurate;
model documentation and result metadata must retain their stated limits.
