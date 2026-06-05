# ADR 0023: Use Documented Synthetic Engineering Defaults

- Status: Accepted
- Date: 2026-05-31

## Context

The reduced-order mechanistic model needs concrete default parameters before implementation. These defaults must be credible enough for simulation and testing, but they must not be presented as field-calibrated production values.

## Options Considered

### Option A: Leave parameters undefined until implementation

- Pros: Avoids premature numeric choices.
- Cons: Blocks implementation and makes model behavior ambiguous.

### Option B: Use arbitrary hardcoded values

- Pros: Fast.
- Cons: Not credible, not reviewable, and difficult to explain.

### Option C: Use documented synthetic engineering defaults with safe ranges

- Pros: Implementation-ready, explainable, bounded, and honest.
- Cons: Still requires future calibration for field realism.

### Option D: Wait for real field data before defining defaults

- Pros: Best eventual realism.
- Cons: Prevents progress and is unnecessary for a simulation-first project.

## Decision

Use **Option C: documented synthetic engineering defaults with safe ranges and units**.

## Rationale

The first digital twin model needs stable, explainable defaults so that model behavior is reproducible and testable. These values are simulation parameters, not real calibrated production values.

## Default Parameters

| Parameter | Unit | Default | Valid Range | Purpose |
| --- | --- | ---: | --- | --- |
| `reservoir_pressure_proxy` | psi | `3200` | `1000-6000` | Approximate pressure source |
| `downstream_pressure` | psi | `250` | `50-1000` | Surface/export pressure |
| `productivity_index` | bbl/day/psi | `1.2` | `0.1-10` | Converts drawdown to inflow |
| `choke_coefficient` | model coefficient | `18` | `1-100` | Converts choke/pressure drop to outflow |
| `pressure_gain_coefficient` | psi per flow imbalance/sec | `0.002` | `0.0001-0.02` | Pressure inventory response |
| `natural_loss_coefficient` | psi/sec | `0.01` | `0-1` | Dissipation/stabilization |
| `ambient_temperature` | degrees F | `85` | `-20-140` | Environmental baseline |
| `thermal_response_rate` | 1/sec | `0.01` | `0.001-0.1` | Temperature lag speed |
| `flow_heating_coefficient` | degrees F | `45` | `0-150` | Flow-driven heating contribution |
| `stress_heating_coefficient` | degrees F | `25` | `0-100` | Pressure/stress heating |
| `water_cut_drift_rate` | fraction/sec | `0.000001` | `0-0.0001` | Slow water cut increase |
| `corrosion_rate_coefficient` | 1/sec | `0.0000005` | `0-0.0001` | Corrosion progression |
| `blockage_growth_coefficient` | 1/sec | `0.0000003` | `0-0.0001` | Flow restriction progression |
| `min_blockage_factor` | fraction | `0.35` | `0.1-1.0` | Prevents unrealistic collapse |
| `max_tubing_pressure` | psi | `5000` | `1000-10000` | Safety clamp |
| `max_flow_rate` | bbl/day | `5000` | `100-50000` | Safety clamp |

## Per-Well Variation

To avoid identical well behavior, each well should receive deterministic variation derived from `wellhead_id` or another stable asset identifier:

- `reservoir_pressure_proxy`: plus or minus `8%`
- `productivity_index`: plus or minus `15%`
- `choke_coefficient`: plus or minus `10%`
- `water_cut`: initial value between `5%` and `35%`
- `corrosion_factor`: starts near `1.0`
- `blockage_factor`: starts between `0.85` and `1.0`

The variation must be deterministic across restarts unless explicitly changed by configuration or calibration.

## Consequences

- The first model implementation has concrete values and validation ranges.
- Documentation must clearly state these are synthetic defaults.
- Future calibration can replace defaults or write per-well overrides.
- Tests can assert that all parameters remain within declared ranges.

