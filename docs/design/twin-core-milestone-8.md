# Milestone 8: Forecasts and Isolated Scenarios

Status: implemented and validated against a disposable TimescaleDB database,
2026-10-13.

## Purpose and scope

The live twin updates each well once per second. Milestone 8 lets a signed-in
user ask what that same model would do over the next few minutes. A forecast
holds current controls fixed. A scenario changes specified controls on a
private copy, while a second private copy supplies an unchanged baseline.
Neither request sends a command to the live well. Live control is Milestone 8B.

This is a synthetic reference well. Its parameters have not been fitted to
measurements from a physical well. A projection is useful for testing model
behavior and comparing synthetic control choices, not for operational decisions
at a real site.

The work implements on-demand requests, four persistence tables, a bounded
heuristic confidence score, and projected alarm thresholds. Historical branching
and the dashboard presentation remain later milestones.

## Decisions and alternatives

[ADR 0013](../adr/0013-prediction-strategy.md) chooses on-demand forecasts first.
Scheduled forecasts would precompute data for a dashboard but add background
load before request behavior is established. [ADR 0014](../adr/0014-scenario-execution.md)
chooses isolated, saved scenarios. An unsaved scenario would use less storage but
would give operators no result to revisit. [ADR 0025](../adr/0025-forecast-horizon-confidence.md)
chooses a short horizon and heuristic confidence. Statistical intervals need
measured forecast errors, which this synthetic reference well does not have.
[ADR 0027](../adr/0027-historical-branching.md) defers historical starts.

## Data flow and trust boundary

```text
signed-in browser
    | session cookie and JSON request
    v
Hono API: capability check, limits, audit
    | service token and numeric actor ID
    v
twin-core: copy one running well under the registry lock
    | state + parameters + controls + model version
    v
private one-second model steps + active historian alarm rules
    | complete run and all sampled points, one database transaction
    v
TimescaleDB
    | API reads saved run and every point by run ID
    v
signed-in browser
```

Only the API knows the internal service token. The browser never calls the
twin-core write routes directly. The API checks the session and the existing
`scenario:run` capability for both creation routes. Saved reads require
`dashboard:read`. The internal service checks a constant-time token comparison
and the actor ID. It refuses requests without a current running well or a
recent model state. The current implementation limits active projections to two
and each HTTP body to 4096 bytes. API creation requests have a separate limit
of ten per client per minute.

## Model and timing

The forecast reuses `step_model` from Milestones 4 through 6. There is one
model, so future outputs and current telemetry share the same equations and
signal meanings. The projection copies a `ProcessState`, `ModelParameters`, and
`ControlInputs` together under the registry lock; it does not read these values
in separate calls while ticks advance.

For each integer second after the copied state:

```text
x[k+1] = F(x[k], u[k], p, t0 + (k+1) seconds)
```

`x` is the `ProcessState`, including pressure in psi, temperature in °F, flow in
barrels per day, and the other modeled quantities. `u` is `ControlInputs`, such
as choke opening in percent and valve booleans. `p` is the fixed
`ModelParameters` for the run. `t0` is `base.state.simulated_at`, the timestamp
of the copied state. `F` is the existing `step_model` function with a one-second
step. The request's `stepSeconds` is an output sampling interval, not a larger
integration step. This matters because stepping the equations in 60-second jumps
would change their behavior.

Forecasts hold `u[k]` equal to copied live controls. For a scenario, an input
scheduled at `offsetMinutes = m` becomes active before the model step ending at
`t0 + (60m + 1) seconds`. Only five actuator-like fields may change: choke
opening, master valve, wing valve, pump, and emergency shutdown. Requested
pressure, flow, gas concentration, or damage outcomes are rejected. The baseline
uses the copied controls for every second, so scenario and baseline points have
the same starting state, parameter set, and timestamps. Their difference comes
only from the requested control schedule.

The horizon `H` is 300 to 7200 seconds, default 1800. The sample interval `S`
is 5 to 300 seconds, default 60. `H` must be divisible by `S`; point count is
`H / S`, at most 1440. A scenario has 1 to 16 nonempty control changes at
strictly increasing whole-minute offsets before the horizon. These limits bound
CPU, memory, response size, and database rows.

## Alarm risk and confidence

Twin-core reads the active `alarmRule` rows and their `parameterType` codes once
per run. It evaluates each rule against the modeled value in that parameter's
canonical unit. The supported operators are `>`, `>=`, `<`, `<=`, and `==`, the
same operators used for historian alarms. A sampled point is `critical` if any
critical rule crosses, `warning` if only warning rules cross, `none` if no rule
crosses, and `unknown` if there are no active rules. The response includes rule
IDs that crossed. A scenario also reports the baseline risk. This is a
projected threshold state, never a probability that an alarm will occur.

The 18 output keys match the existing historian `parameterType` codes and
units. Pressure keys (`tubing_pressure`, `casing_pressure`,
`annulus_pressure`) use psi. `flow_rate` uses barrels/day;
`gas_oil_ratio` uses standard cubic feet per stock-tank barrel. `sand_detector`
and `h2s_level` use parts per million; `corrosion_rate` uses mils per year;
`co2_level`, `choke_valve_position`, and `water_cut` use percent;
`vibration` uses millimeters/second; `wellhead_temperature` uses °F.
`master_valve_status`, `wing_valve_status`, `swab_valve_status`,
`emergency_shutdown`, and `pump_status` are 0 or 1. Choke position is rounded
to the integer value that the Modbus gateway publishes before comparing an
alarm threshold. A parity test compares all projected values with the gateway
conversion from the same model state.

The confidence number is an explicit, uncalibrated heuristic for the whole run:

```text
C = clamp(0.65 - 0.25 * H / 7200 - 0.10*R - 0.10*A - 0.10*U, 0.10, 0.65)
```

`C` is `confidence.score`, a dimensionless display score. `H` is
`horizonSeconds` in seconds. `R` is 1 when the copied model was recently
restored. `A` is 1 if any projected value lies within ten percent of an active
rule threshold, or if there are no active alarm rules. `U` is 1 if a scheduled
choke move exceeds 25
percentage points or a binary actuator changes. The code records separate
reason strings for every deduction. The formula is implemented by `_confidence`
in `twin_core/projection.py`; the no-rule deduction is separate from the
near-threshold deduction. The label is `moderate` for scores at least 0.4 and
`low` otherwise. All results say `heuristic_uncalibrated` and
`fieldCalibrated: false`. No output calls this a confidence interval.

The score cannot account for measured forecast error, parameter uncertainty,
unknown equipment faults, or changing external conditions. It must not be used
to rank real-world safety. A later calibration project needs field history and
backtesting before it can assign statistical meaning.

## Persistence and retrieval

Migration `0011_forecasts_and_scenarios.sql` adds ordinary `prediction_run` and
`scenario_run` tables. Each stores a UUID, well ID, requesting actor ID, model
version, parameter fingerprint, copied state, parameter values, control values,
active alarm rules, horizon, sample interval, and confidence result. Scenario
runs also store the name and scheduled control changes. These fields make a run
explainable after model defaults or alarm rules change. The actor ID references
`users(id)`, so a saved run cannot claim a nonexistent account.

`prediction_point` and `scenario_point` are TimescaleDB hypertables partitioned
by `forecast_at`. Their primary key includes `(run_id, forecast_at)`, so it
contains the partitioning time. Each point stores all 18 canonical model signals
as JSONB, queryable tubing pressure (psi), flow (barrels/day), wellhead
temperature (°F), water cut (percent), and risk. Scenario points also store
baseline values and baseline risk.
This key shape follows
[TimescaleDB's unique-index rule](https://docs.timescale.com/use-timescale/latest/hypertables/hypertables-and-unique-indexes/).
The two point tables are compressed after 3 and 7 days and retained for 30 and
90 days respectively. Run metadata has no automated expiry. Once any point is
missing, the public read endpoint returns HTTP 410 rather than a truncated run.
This avoids pretending that a retained fragment is the original result.

Twin-core writes the run and every point in one database transaction. If any
insert fails, PostgreSQL rolls the whole run back and the API returns an error.
The twin database role can read active alarm rules and insert projection rows;
it has no update or delete grant on these tables. The API uses the existing
database reader to load saved runs, ordered by sample time.

## API and failure behavior

The internal routes are `POST /wellheads/{wellheadId}/forecast` and
`POST /scenarios`; successful creation returns 201 with a run and all points.
The public routes are `POST /twin/wellheads/{wellheadId}/forecast`,
`POST /twin/scenarios`, `GET /twin/forecasts/{runId}`, and
`GET /twin/scenarios/{runId}`. The public API wraps successful data in its
standard envelope and returns HTTP 200. The OpenAPI contracts specify the
request and response fields.

Invalid inputs return 400. Missing session or capability returns 401 or 403.
Unknown wells and runs return 404. An expired or incomplete saved run returns
410. A busy or unavailable model or database returns 503; the API gives its
internal call 12 seconds before a 504 timeout. The API records success or
failure of creation in its audit trail without recording the service token.
Twin-core counts runs by kind and outcome and measures their duration.

## Verification and remaining limits

Tests cover model isolation, control timing and direction, bounds, active-rule
projection, internal authentication and failures, public authentication and
saved reads, and migration application on a disposable Timescale database.
The final verification result belongs in the delivery report and should not be
inferred from this design alone.

No dashboard forecast or scenario view is part of this milestone. No physical
well data was supplied for calibration or external validation. There is no
historical branch, background forecast scheduler, or live control command here.
