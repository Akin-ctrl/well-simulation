# ADR 0025: Use Short Operational Forecasts With Heuristic Confidence

- Status: Accepted
- Date: 2026-05-31
- Implementation: Implemented as an uncalibrated run-level heuristic

## Context

The twin should forecast future process behavior, but the first model is a reduced-order mechanistic model using synthetic defaults. Forecasts must be useful without implying field-calibrated certainty.

## Options Considered

### Option A: Long-horizon forecasts

- Pros: Impressive for demos.
- Cons: Not credible without calibration, richer physics, and historical error validation.

### Option B: Short operational forecasts

- Pros: More credible and useful for near-term operational awareness.
- Cons: Less dramatic than long-term forecasts.

### Option C: No confidence representation

- Pros: Simple.
- Cons: Hides uncertainty and weakens decision support.

### Option D: Heuristic confidence

- Pros: Honest, implementable, and useful before statistical calibration exists.
- Cons: Not a calibrated statistical interval.

### Option E: Statistical confidence intervals

- Pros: More rigorous.
- Cons: Requires historical forecast error data that the project does not yet have.

## Decision

Use **short operational forecasts with heuristic confidence**.

## Forecast Defaults

- Default horizon: `30 minutes`
- Maximum horizon: `2 hours`
- Default step size: `60 seconds`
- Minimum step size: `5 seconds`
- Maximum step size: `5 minutes`

## Confidence Model

The first confidence model is heuristic and must be explicitly labeled as non-statistical.

Forecast confidence should decrease when:

- forecast horizon is longer
- model recently recovered from downtime
- model status is `degraded`, `lagging`, or `recovering`
- parameters are defaults rather than calibrated
- process state is near alarm thresholds
- controls change sharply
- operating conditions are unstable

## Suggested Confidence Fields

At the forecast-run level:

- `confidence_method`
- `confidence_score`
- `confidence_label`
- `confidence_reasons`

At the forecast-point level:

- `alarm_risk`
- optional future point-level confidence score

## Consequences

- Forecasts are useful for near-term operational awareness without overclaiming.
- The dashboard should label heuristic confidence clearly.
- Historical forecast error tracking can later support calibrated confidence intervals.

## Implementation on 2026-10-13

Milestone 8 implements the defaults and limits above, with a 300-second minimum
horizon. It uses the existing one-second model step and samples output at the
requested interval. The run-level score starts at 0.65, falls by up to 0.25
with horizon, and deducts 0.10 each for recent recovery, being near a rule
threshold or having no active rules, and a sharp control change. It is clamped
to 0.10 through 0.65. Every response labels the method
`heuristic_uncalibrated` and `fieldCalibrated: false`. A rule crossing is a
categorical risk label, not a probability. Model status must be running and
fresh before a run starts; non-running states are refused instead of being
assigned a lower score. The exact formula and units are in the
[Milestone 8 design](../design/twin-core-milestone-8.md).

