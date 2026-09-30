# Milestone 5: Reduced-Order Wellhead Model

## Purpose and boundary

Milestone 5 gives each active well a deterministic process state. It models cause
and effect for pressure, flow, temperature, water cut, and slow degradation. All
numbers are synthetic and uncalibrated. The model supports a demonstration and
future what-if calculations; it is not a reservoir forecast or safety system.

The existing random simulator still feeds Modbus and the historian. Milestone 6
will connect model output to that path. Until then, `GET /wellheads/{id}/state`
returns model state with `source: synthetic_reduced_order_model` and
`fieldCalibrated: false`. `GET /wellheads/{id}/latest-telemetry` returns
separately labelled simulator readings. The two must never be presented as the same source.
No HTTP control write or physical actuator command is added in this milestone.

## System design

```text
versioned defaults -> stable per-well variation -> global overrides
                   -> future database overrides -> typed model parameters
active well IDs -----------------------------------> one state per well
fixed one-second scheduler -> pure model step -----> internal state read API
current simulator -> Modbus -> ingestion -> historian -> latest-telemetry API
```

The model step is a pure function: a state, control inputs, parameters, and a
one-second duration give exactly one next state. It reads no clock or database.
The registry owns the live states. A new well begins from a synthetic operating
balance. An existing well keeps its state during a fleet refresh if its
parameters are unchanged. Every parameter currently requires restart. A changed
parameter marks that well unavailable rather than silently changing its physics.

## Inputs, units, and equations

`P_r` is `reservoir_pressure_proxy_psi` (psi), a fixed pressure source.
`P_t` is `tubing_pressure_psi` (psi), the stored upstream pressure. `P_d` is
`downstream_pressure` (psi). `q_in` and `q_out` are barrels per day (bbl/day).
`dt_seconds` is exactly one second in the live runtime. The coefficients below
come from `model_defaults.json` and may be overridden only through validation.

### Reservoir inflow and choke outflow

```text
q_in = PI * max(P_r - P_t, 0)
q_out = min(q_max, C * u * v * f_p * b * sqrt(max(P_t - P_d, 0)))
```

`PI` is `productivity_index` in bbl/day/psi. `C` is `choke_coefficient` in
bbl/day/sqrt(psi). `u` is `choke_position_percent / 100`, from zero to one.
`v` is one only when the master and wing valves are open and emergency shutdown
is false; otherwise it is zero. `f_p` is `pump_flow_factor`, a dimensionless
state between `pump_off_flow_factor` and one. `b` is `blockage_factor`, the
remaining dimensionless capacity. `q_max` is `max_flow_rate` in bbl/day.
The swab valve is recorded but is not in this production flow path. Its position
does not command any real hardware.

The productivity-index relationship normally uses reservoir-to-sandface
pressure drawdown. This model has no bottomhole measurement, so `P_t` stands in
for flowing pressure. That substitution is an explicit synthetic approximation,
not a way to estimate a real well's productivity index. The square-root outflow
term follows the steady, single-fluid orifice shape. The coefficient combines
geometry and fluid effects that this model does not calculate. Multiphase flow,
compressibility, and a calibrated choke curve are outside this milestone.

Pump effectiveness moves toward one when on and toward `pump_off_flow_factor`
when off:

```text
f_p_next = f_p + pump_response_rate * (target - f_p) * dt_seconds
```

`pump_response_rate` has unit 1/second. The outflow calculation uses `f_p_next`,
so pump-off begins reducing flow on the first step and continues over time.

### Pressure and casing response

```text
P_t_next = clamp(P_t + [k_p * (q_in - q_out) - k_l] * dt_seconds,
                 0, max_tubing_pressure)
P_c_target = P_d + casing_pressure_fraction * (P_t_next - P_d)
P_c_next = P_c + casing_response_rate * (P_c_target - P_c) * dt_seconds
```

`k_p` is `pressure_gain_coefficient` in psi per (bbl/day) per second.
`k_l` is `natural_loss_coefficient` in psi/second. `P_c` is
`casing_pressure_psi` (psi). `casing_pressure_fraction` is dimensionless and
`casing_response_rate` has unit 1/second. These equations are inventory and
response proxies, not a multiphase wellbore calculation. Closing a choke lowers
outflow immediately, then raises upstream pressure because inflow exceeds
outflow. A fully closed path can therefore retain pressure.

### Temperature, water, and degradation

```text
flow_ratio = clamp(q_out / max_flow_rate, 0, 1)
pressure_ratio = clamp(P_t_next / max_tubing_pressure, 0, 1)
T_target = ambient_temperature
           + flow_heating_coefficient * flow_ratio
           + stress_heating_coefficient * pressure_ratio
T_next = T + thermal_response_rate * (T_target - T) * dt_seconds
w_next = clamp(w + water_cut_drift_rate * (1 + pressure_ratio)
               * dt_seconds, 0, 1)
c_next = clamp(c + corrosion_rate_coefficient * w_next
               * (1 + pressure_ratio) * dt_seconds,
               1, max_corrosion_factor)
b_next = clamp(b - blockage_growth_coefficient
               * (w_next + flow_ratio) * dt_seconds,
               min_blockage_factor, 1)
```

`T` and `T_target` are degrees Fahrenheit. `thermal_response_rate` is 1/second;
the heating coefficients are degrees Fahrenheit. `w` is water fraction, shown
as `waterCutPercent` after multiplying by 100. Its drift coefficient is
fraction/second. `c` is `corrosion_factor`, an increasing dimensionless damage
index, and `b` is `blockage_factor`. Their coefficients are 1/second. Water and
flow are only risk proxies because no sand, H2S, or CO2 measurements feed this
model. The values must not be interpreted as a corrosion rate for real pipe.

### New documented parameters

| Key                          | Unit     | Default | Allowed range | Why it exists                              |
| ---------------------------- | -------- | ------: | ------------- | ------------------------------------------ |
| `pump_off_flow_factor`     | fraction |    0.55 | 0 to 1        | Residual natural flow when the pump is off |
| `pump_response_rate`       | 1/sec    |    0.03 | 0.001 to 0.2  | Pump flow response speed                   |
| `casing_pressure_fraction` | fraction |    0.65 | 0 to 1        | Casing target relative to tubing pressure  |
| `casing_response_rate`     | 1/sec    |    0.02 | 0.001 to 0.2  | Casing pressure lag                        |
| `max_corrosion_factor`     | index    |       2 | 1 to 5        | Bound on the synthetic damage index        |

The other 16 values and ranges remain in ADR 0023 and the versioned JSON file.
The model version becomes `0.2.0` because the state and parameter contract changes.

## Initialization and per-well variation

For an open choke and healthy valves, initial tubing pressure is found by
bisection between downstream and reservoir pressure until inflow and outflow
balance. If no balance exists below the configured pressure bound, the well
becomes degraded instead of reporting a false equilibrium. The search uses a
fixed number of steps, so the same parameters produce the same result on every
host. Initial casing pressure and temperature use their
target formulas at that operating point. Pump factor begins at one.

A stable digest of the positive well ID and variation key makes
a number from zero to one. It gives reservoir pressure plus or minus 8%,
productivity index plus or minus 15%, and choke coefficient plus or minus 10%.
Initial water cut lies between 5% and 35%; blockage capacity lies between 0.85
and 1; corrosion index begins near 1. Values remain within declared bounds.
Global and per-well overrides take precedence over this variation. Python's
process-randomized `hash()` is never used.

This synthetic initialization is a new run, not a recovered history. Until
Milestone 7 adds snapshots, every restart returns to that initial balance. The
state reason exposes `cold_start_no_snapshot` so a consumer cannot mistake it
for continuous plant history.

## Tick order and failure handling

One tick validates controls and state, moves pump effectiveness, estimates inflow
and outflow, updates tubing and casing pressure, then updates temperature, water
cut, corrosion, and blockage. All outputs must be finite and inside their bounds.
A well with invalid parameters stays unavailable. A corrected well that has
never run can initialize on the next fleet refresh. If it ran before the invalid
change, it requires restart. A calculation failure marks that well degraded and
preserves its last valid process state. Readiness fails when any active well is
not running. Fleet readiness also names `lagging` when ticks are stale and
`degraded` when only some wells run.

The scheduler still runs at most five one-second catch-up steps by default. It
never applies a large elapsed-time step. Skipped ticks increase a metric and a
per-well gap count. A gap moves the displayed simulation timestamp to the new
tick without pretending that the skipped seconds were integrated. Source clock
time and service tick time remain distinct. No missed history is written.

## Alternatives and costs

A constant pump multiplier would be simpler but would make pump-off an instant
jump. A first-order response gives a visible, bounded decline. A detailed pump
curve would need pump and fluid data that this project does not have.

Initializing at downstream pressure would create a long artificial startup
transient. Solving the simplified balance gives a stable synthetic starting
point, but it does not reproduce a measured well state. Snapshot restoration in
Milestone 7 is still required for continuity.

The model could immediately replace the Modbus simulator. Keeping that change
in Milestone 6 lets the pure process model be tested before industrial protocol
mapping changes. The cost is two clearly labelled synthetic sources for now.

## Acceptance checks

- Unit tests prove deterministic variation, override precedence, and bounds.
- Monotonic tests prove choke closure lowers flow and raises upstream pressure,
  blockage lowers outflow, and pump-off lowers flow over repeated steps.
- Tests cover valve and shutdown closures, thermal lag, degradation bounds,
  invalid controls, and non-finite state failure.
- Registry and API tests verify running state, cold-start reason, separate
  simulator telemetry, and explicit degraded/unavailable behavior.
- Ruff, formatting, strict mypy, Python tests, OpenAPI validation, and prose
  checks pass. A rebuilt twin-core container runs all active demo wells and
  remains healthy while the existing Modbus and historian services continue.

## Technical reference check

[SLB&#39;s productivity-index definition](https://glossary.slb.com/terms/p/productivity_index_pi)
uses delivered volume per psi of drawdown at the sandface. That supports the
unit and linear drawdown form above, while also showing why a wellhead pressure
proxy cannot be presented as a field PI measurement.

[A NIST orifice-flow study](https://nvlpubs.nist.gov/nistpubs/jres/2/jresv2n3p561_a2b.pdf)
reports near square-root dependence on pressure difference for steady liquid
flow through an orifice. The model uses that shape only. Its choke coefficient
is synthetic; no claim of NIST meter calibration, multiphase accuracy, or field
fitness follows from using the same mathematical form.

## Verification on 2026-09-29

The full Python suite passed 116 tests. Ruff lint and formatting passed, and
strict mypy found no issues in 30 source files. Both OpenAPI specifications
validated. The prose checker reported zero errors and warnings.

The twin-core image built and was restarted against the existing demo database.
All 12 active wells returned running state with model version `0.2.0`, explicit
synthetic source, and `fieldCalibrated: false`. The internal readiness endpoint
returned ready. The model-step metric was present. The historian endpoint still
reported `current_simulator_via_historian` with `modelGenerated: false`. The
other Compose services remained healthy, and the migrator exited with code 0.
A new fresh-volume test was not run for Milestone 5; no migration or database
schema changed in this milestone.

## Standards review

Inputs and coefficients have typed boundaries, declared units, finite checks,
valid ranges, and fixed one-second integration. A bad model step leaves the last
valid state visible with degraded status. Each skipped scheduler second is
counted without inventing process history. Synthetic output and historian
readings carry different source labels. Logs omit credentials and raw payloads;
metrics use fixed names without well IDs as labels. The service still has no
control write endpoint or physical device connection. A restart begins a new
synthetic run until durable snapshots arrive in Milestone 7.

## Milestone 6 handoff, 2026-09-30

Milestone 6 replaced the gateway's random subprocess with model output.
The [Milestone 6 design](twin-core-milestone-6.md) records the complete 18-signal
mapping, Modbus freshness handshake, and historian source labels. Version 0.3.0
recalculates published flow at the updated tubing pressure, then derives phase
and diagnostic readings from that same state. The pressure step still uses the
starting flow. The [reference-well design](reference-well-physics.md) records
those additional equations and their limits. The earlier
boundary description above records what was true when Milestone 5 finished.
