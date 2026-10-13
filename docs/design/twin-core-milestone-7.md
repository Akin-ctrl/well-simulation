# Milestone 7: Twin Persistence

Status: implemented and verified against the local Docker stack, 2026-10-12.

## Purpose

Twin-core keeps each well's active process state in memory. Before this
milestone, a restart lost that state. Every well started again from its
synthetic operating balance, reported as `cold_start_no_snapshot`, and lost
the water cut, corrosion, and blockage that had accumulated.

A concrete case shows the goal. Twin-core runs from 08:00 and restarts at 10:00.
It now loads each well's state saved at about 09:59:30,
records that the model did not run for the missing time, and carries on. The
restart becomes a visible, recorded gap instead of a silent reset.

The model stays synthetic and uncalibrated. Persistence makes it continuous; it
does not make it more accurate.

## Decisions this design follows

Already accepted, and not reopened here:

- [ADR 0012](../adr/0012-state-storage.md): live state stays in memory, with
  periodic snapshots in TimescaleDB.
- [ADR 0020](../adr/0020-model-tick-semantics.md): one-second fixed steps, and
  30-second state snapshots.
- [ADR 0022](../adr/0022-recovery-semantics.md): restore the latest snapshot,
  catch up small gaps, record larger ones as recovery events, and report
  `recovering` before `running`.
- [ADR 0024](../adr/0024-twin-persistence-schema.md): important fields as
  columns, the full state as JSONB, time series as hypertables.
- [ADR 0026](../adr/0026-retention-and-compression.md): snapshots compressed
  after 7 days and kept 90 days; recovery events kept 2 years.
- [ADR 0021](../adr/0021-model-parameterization.md): version-controlled defaults
  with per-well database overrides.

Decided on 2026-10-03 for this milestone, and recorded as amendments to ADRs
0021, 0022, and 0024:

- Only the tables this milestone writes are created.
- After a long gap, slow damage is restored as saved and fast states settle
  toward balance by how much time passed.
- Twin-core gets insert-only access to its two new tables.
- Per-well overrides are edited by SQL for now.
- The live control API, and its `control_command` table, move to a new
  milestone after Milestone 8.

## Scope

**In:**

- `twin_state_snapshot`, `twin_recovery_event`, and `wellhead_model_parameter`
  tables, with retention and compression.
- A snapshot writer in twin-core that never blocks the tick loop.
- Restore on startup, including the graded settling rule below.
- Recovery events for restores and for the wall-clock jumps added on
  2026-10-03.
- A database-backed per-well override source replacing `NoDatabaseOverrides`.
- Metrics, readiness, the internal API, and the Drizzle mirror for the new
  tables.

**Out, and where it goes instead:**

- `prediction_run` and `prediction_point`: Milestone 8, with forecasts.
- `scenario_run` and `scenario_point`: Milestone 8, with what-if scenarios.
- `control_command`: the new control milestone after Milestone 8.
- `twin_model_definition` and `wellhead_model_config`: not needed while the
  code runs one model version. Each snapshot and reading records the version it
  came from, which Milestone 8 can join to a definition table if one is added.
- An API or screen for editing overrides: the control milestone, which builds
  the authenticated write path anyway.
- Showing recovery gaps on the dashboard: Milestone 9.

## Data flow

```text
                      every 30 s, latest only
tick loop ──fleet copy──> [one-slot mailbox] ──> snapshot writer ──INSERT──> twin_state_snapshot
    │                                                  ▲
    └── clock jump, restore outcome ──> [event queue, 100] ┘ ──INSERT──> twin_recovery_event

startup: wellhead_model_parameter ──> resolve parameters ──> fingerprint
         twin_state_snapshot (latest per well) ──> compatible? gap? ──> restored state
```

The tick loop only ever hands work over. It never waits for the database.

## Schema

All three tables are created by migration `0010_twin_persistence.sql`.
They reuse the `model_version_text` domain from
migration 0008, which accepts only `major.minor.patch`.

### `twin_state_snapshot` (hypertable on `simulated_at`)

| Column | Type | Meaning |
| --- | --- | --- |
| `snapshot_id` | BIGSERIAL | Row identity |
| `simulated_at` | TIMESTAMPTZ NOT NULL | Model tick time of the saved state |
| `wellhead_id` | INT NOT NULL, references `wellhead` | The well |
| `model_version` | `model_version_text` NOT NULL | Model that produced the state |
| `parameter_fingerprint` | TEXT NOT NULL | SHA-256 of the resolved parameters, see below |
| `status` | TEXT NOT NULL | Well status when saved, constrained to `running` |
| `tick_index` | BIGINT NOT NULL | Ticks since this process run started |
| `skipped_ticks` | BIGINT NOT NULL | Gap ticks recorded so far |
| `tubing_pressure_psi` | DOUBLE PRECISION NOT NULL | High-value column |
| `casing_pressure_psi` | DOUBLE PRECISION NOT NULL | High-value column |
| `flow_rate_bpd` | DOUBLE PRECISION NOT NULL | High-value column |
| `wellhead_temperature_f` | DOUBLE PRECISION NOT NULL | High-value column |
| `water_cut_percent` | DOUBLE PRECISION NOT NULL | High-value column |
| `choke_position_percent` | DOUBLE PRECISION NOT NULL | High-value column |
| `blockage_factor` | DOUBLE PRECISION NOT NULL | High-value column |
| `corrosion_factor` | DOUBLE PRECISION NOT NULL | High-value column |
| `state` | JSONB NOT NULL | The full process state and controls, as `to_payload()` returns them |
| `inserted_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | Database arrival time |

Primary key `(snapshot_id, simulated_at)`, as TimescaleDB requires the time
column in it. Index on `(wellhead_id, simulated_at DESC)` for the "latest per
well" read at startup. Compression segments by `wellhead_id`, orders by
`simulated_at DESC`, starts after 7 days. Retention drops chunks after 90 days.

At one row per well every 30 seconds, 12 wells write 34,560 rows a day, about
3.1 million over 90 days. Restore reads the latest row, including when the
service has been stopped long enough for that row to be compressed.

**Parameter fingerprint.** The resolved parameter values for the well, sorted by
key and written as compact JSON, then hashed with SHA-256 and stored as hex.
Two runs with the same model version but a changed override get different
fingerprints, so a snapshot made under other parameters is never restored
silently.

### `twin_recovery_event` (hypertable on `occurred_at`)

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | BIGSERIAL | Row identity |
| `occurred_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | When it was recorded |
| `wellhead_id` | INT NULL, references `wellHead` | The well; NULL for a fleet-wide event such as a clock jump |
| `event_type` | TEXT NOT NULL, check constraint | One of the types below |
| `model_version` | `model_version_text` NOT NULL | Running model version |
| `snapshot_simulated_at` | TIMESTAMPTZ NULL | The restored or rejected snapshot, when there is one |
| `gap_seconds` | DOUBLE PRECISION NULL | Missing model time |
| `details` | JSONB NOT NULL DEFAULT '{}' | Reason codes, such as which check a snapshot failed |

Event types:

| Type | When |
| --- | --- |
| `snapshot_restored` | A compatible snapshot was loaded |
| `cold_start_no_snapshot` | The well has no snapshot |
| `snapshot_incompatible` | Model version or parameter fingerprint differs; the well cold-starts |
| `clock_jump_forward` | The wall clock moved ahead of tick time (the 2026-10-03 fix) |
| `clock_jump_backward` | The wall clock moved back; ticks held |

Retention 2 years (ADR 0026). No compression: the volume is a handful of rows
per restart. It is a hypertable only so the retention policy can apply.

### `wellhead_model_parameter`

| Column | Type | Meaning |
| --- | --- | --- |
| `parameter_override_id` | SERIAL PRIMARY KEY | Row identity |
| `wellhead_id` | INT NOT NULL, references `wellHead` | The well |
| `model_version` | `model_version_text` NOT NULL | Model version the value was set for |
| `parameter_key` | TEXT NOT NULL | A key from `model_defaults.json` |
| `value` | DOUBLE PRECISION NULL | The override; NULL means "back to the default" |
| `unit` | TEXT NULL | Must equal the parameter's unit; NULL only when `value` is NULL |
| `source` | TEXT NOT NULL | Where the value came from, such as `manual` |
| `changed_by` | TEXT NOT NULL | Who made the change, typed by hand |
| `reason` | TEXT NOT NULL | Why |
| `valid_from` | TIMESTAMPTZ NOT NULL DEFAULT now() | When it takes effect |

Unique on `(wellhead_id, model_version, parameter_key, valid_from)`. Rows are
never updated or deleted: a change is a new row, and reverting is a row with
`value` NULL. The current override for a key is the newest row whose
`valid_from` has passed. A check constraint keeps `value` and `unit` both set or
both NULL. This keeps every past value, with who changed it and why, as the
typed record that stands in for an audit trail until the control milestone.

### Access

The twin role, created by the migrator, keeps its existing `SELECT` grants and
gains:

- `SELECT` on `wellhead_model_parameter` and `twin_state_snapshot`;
- `INSERT` on `twin_state_snapshot` and `twin_recovery_event`, and `USAGE` on
  their sequences.

No `UPDATE` or `DELETE` anywhere, so twin-core can add history but never change
or remove it. Its reader connections stay forced read-only. Only the writer's
connection is opened without that setting.

## Snapshot writer

- Every 30 seconds of tick time, the tick loop copies the registry snapshot
  (already an immutable tuple) and puts it in a one-slot mailbox.
- A single writer thread takes the copy and inserts one row per well with
  running process state, in one transaction, with a 3-second connect and
  statement timeout.
- If the writer is still busy when the next copy arrives, the older copy is
  replaced, never queued. Each replacement is counted. The newest state is all
  restore needs, so a replaced copy loses nothing that matters.
- A failed write is logged and counted, and the next copy is tried 30 seconds
  later. There is no retry loop.
- On a clean shutdown (SIGTERM), the model loop stops before taking its final
  copy. The shutdown path writes that copy directly using a separate bounded
  database connection, so an older write or queued event cannot delay it.
  A failed final write is logged and counted, and the process exits nonzero.
  The background writer then gets up to 3 seconds to finish queued events.
- Recovery events go through a separate bounded queue of 100, written by the
  same thread. Events are not replaceable, so an overflow is logged at ERROR
  and counted rather than dropped quietly.

## Restore on startup

After the first fleet refresh, and before the first tick, twin-core reads the
latest snapshot for each active well and decides per well.

1. **No snapshot:** cold start from the operating balance, as today. Event
   `cold_start_no_snapshot`.
2. **Incompatible:** the model version or the parameter fingerprint differs.
   Cold start. Event `snapshot_incompatible`, with the failed check in
   `details`.
3. **Compatible:** let the gap $g$ be wall time now minus the snapshot's
   `simulated_at`, in seconds.
   - $g < 0$: the wall clock is behind the snapshot. Restore as saved and let
     the existing hold logic wait until the wall clock passes it.
   - $0 \le g \le 5$: restore as saved, then run $\lfloor g \rfloor$ real
     one-second steps. This is the bounded catch-up ADR 0022 allows; 5 is the
     existing `TWIN_MAX_CATCHUP_STEPS`.
   - $g > 5$: restore using the settling rule below. Record $\lfloor g \rfloor$
     skipped ticks.

   Event `snapshot_restored`, with $g$ in `gap_seconds`.

The well reports `recovering` until its first tick after restore, then
`running` with reason `restored_from_snapshot`. No readings are produced for
the gap, so the historian shows it as missing data.

### Settling rule for gaps over 5 seconds

The model did not run during the gap, but a real well would have kept going.
Its fast states would have moved toward balance, and its slow damage would have
kept accumulating at the rate it had.

**Slow damage is restored exactly as saved:** `water_cut_percent`,
`corrosion_factor`, `blockage_factor`. They change over hours to days (water
cut drifts about 0.36 percentage points an hour), and they are what a snapshot
exists to keep. They are not advanced through the gap, because that would
invent model history the model never computed.

**The balance is re-solved** with that restored damage and the current controls
and parameters. The solver is the one `initial_state` already uses.

**Each fast state then moves from its saved value toward that balance** by the
fraction it would have settled in the gap:

$$x_{\text{restored}} = x_{\text{bal}} + (x_{\text{saved}} - x_{\text{bal}})\, e^{-g/\tau}$$

| Symbol | Meaning | Unit | Where it comes from |
| --- | --- | --- | --- |
| $x_{\text{saved}}$ | The state in the snapshot | the state's unit | `twin_state_snapshot.state` |
| $x_{\text{bal}}$ | The same state at the re-solved balance | the state's unit | balance solver |
| $g$ | Gap | s | now minus `simulated_at` |
| $\tau$ | The state's time constant | s | table below |

| Fast state | $\tau$ | Source in code |
| --- | --- | --- |
| Tubing pressure | $1 / \big(k\,(J + \partial Q/\partial P)\big)$, about 6 minutes | see below |
| Casing pressure | $1 / r_c$ = 50 s | `casing_response_rate` |
| Wellhead temperature | $1 / r_T$ = 100 s | `thermal_response_rate` |
| Annulus temperature | $1 / r_A$ = 200 s | `annulus_thermal_response_rate` |
| Pump flow factor | $1 / r_p$ = 33 s | `pump_response_rate` |

For tubing pressure, the model's pressure step is
$P_{n+1} = P_n + \big(k\,(J\,(P_r - P_n) - Q(P_n)) - L\big)\,\Delta t$.
Linearising around the balance gives the time constant above:

| Symbol | Meaning | Unit | Code |
| --- | --- | --- | --- |
| $k$ | Pressure gain | psi per (bbl/day) per s | `pressure_gain_coefficient`, 0.002 |
| $J$ | Productivity index | bbl/day/psi | `productivity_index`, 1.2 |
| $\partial Q/\partial P$ | How fast choke flow rises with pressure at balance | bbl/day/psi | $C\,(u/100)\,f\,b \,/\, \big(2\sqrt{P - P_d}\big)$ |
| $C$ | Choke coefficient | bbl/day/√psi | `choke_coefficient` |
| $u$ | Choke opening | % | `choke_position_percent` |
| $f$ | Pump flow factor | dimensionless | `pump_flow_factor` |
| $b$ | Blockage factor | dimensionless | `blockage_factor` |
| $P_d$ | Downstream pressure | psi | `downstream_pressure` |

Checked against the real model on 2026-10-03: the formula gives 360 s and
343 s for wells 1 and 7, and a direct step-response measurement gives 359 s and
342 s. A fast state is within 1% of balance after about $5\tau$: about
27 minutes for tubing pressure and under 17 minutes for the others.

**Annulus pressure is not graded.** The annulus holds a fixed mass of gas, so
its pressure follows its temperature. It is recalculated from the restored
annulus temperature with the fixed-mass gas law the model already uses
(`annulus_next_pressure`). **Flow and the seven diagnostic readings** have no
memory of their own; they are recomputed from the restored state exactly as on
every tick.

A pure function, `restore_state(saved, parameters, controls, gap_seconds, at)`,
returns the restored `ProcessState`. It holds no clock or database access, so
every branch can be tested directly.

## Per-well overrides

`DatabaseOverrideSource.load(wellhead_ids, model_version)` replaces
`NoDatabaseOverrides`. It reads the newest row per well and key where
`valid_from` has passed, drops rows whose `value` is NULL, and returns
`ParameterOverride(value, unit, source)`. The existing resolver then checks the
key, the unit, the source, and the range. The loader runs on every fleet
refresh (30 seconds), so a change is picked up within 30 seconds:

- an invalid value marks that well `unavailable`, reason `invalid_parameters`,
  and readiness reports it;
- a valid but changed value on a running well marks it `restart_required`,
  which is the existing behaviour. Live behaviour never changes silently.

An administrator edits overrides with SQL, as documented in `data/README.md`.
The `changed_by` and `reason` columns are typed by hand, so they are a record,
not a verified actor. That is accepted until the control milestone builds an
authenticated write path (ADR 0021 amendment).

## Status, readiness, and the internal API

- Well status gains `recovering`, between restore and the first tick.
- `GET /wellheads/{id}/state` gains `restoredFrom` (snapshot time or null) and
  `lastRecoveryEvent` (type and time).
- Readiness is unchanged in rule: a well that is `recovering` is not running,
  so readiness waits for one tick, about a second.

## Failure behaviour

| Condition | Behaviour |
| --- | --- |
| Database down at startup | Snapshot read fails. The fleet refresh already fails in this case, so the service is not ready and retries. It never cold-starts by mistake because a read failed. |
| Database down while running | Snapshot writes fail and are counted. Ticks continue. Restore after a restart uses the last snapshot that was saved. |
| Writer slower than 30 seconds | The pending copy is replaced and counted. Ticks never wait. |
| Event queue full | Overflow logged at ERROR and counted. |
| Snapshot from another model version or parameters | Not restored. Cold start, with the reason recorded. |
| Snapshot JSON fails validation | Treated as incompatible. The well cold-starts; the bad row is reported in `details`. |
| Saved state fails the model's bounds check | The snapshot is rejected as incompatible and the well starts cold. |
| Twin role lacks a grant | The migrator grants it before twin-core starts. A missing grant fails the write, which is counted and logged. |

## Metrics

- `well_sim_twin_snapshot_writes_total` and
  `well_sim_twin_snapshot_write_failures_total`.
- `well_sim_twin_snapshots_replaced_total`, copies replaced before writing.
- `well_sim_twin_last_snapshot_age_seconds`.
- `well_sim_twin_recoveries_total{outcome}`, labelled with the event type.
- `well_sim_twin_recovery_events_dropped_total`.

## Acceptance checks

Each check is a test, unless it says "live".

1. A restart less than 5 seconds after the last snapshot runs that many real
   steps, records no gap, and continues the saved trajectory.
2. A restart after 10 minutes keeps water cut, corrosion, and blockage exactly
   as saved. Tubing pressure is
   $x_{\text{bal}} + (x_{\text{saved}} - x_{\text{bal}}) e^{-600/\tau}$, within
   floating-point tolerance. 600 skipped ticks are recorded.
3. A restart after 9 hours leaves every fast state within 1% of balance, and
   the damage is still as saved.
4. A changed override, or a new model version, prevents restore and records
   `snapshot_incompatible` with the reason.
5. The tick loop's timing does not change while the database is unreachable.
6. A slow writer replaces pending copies and never blocks a tick.
7. The twin role cannot `UPDATE` or `DELETE` any table (tested against a real
   database).
8. An invalid override marks only its well unavailable; a valid change marks a
   running well `restart_required`.
9. Live: restart twin-core and see the wells resume, not cold-start, with a
   `snapshot_restored` event and the gap visible in the historian.
10. Live: put the host to sleep and see a `clock_jump_forward` event recorded.

## What this design deliberately does not do

- **Replay the gap.** Running thousands of steps to fill a downtime would
  present model history that was never computed. ADR 0022 rejects it.
- **Advance slow damage through the gap.** For the same reason. A long
  outage shows up as recorded missing time, not as extrapolated wear.
- **Restore across model versions.** A newer model may read the same field
  differently. A cold start with a recorded reason is safer than a guess.

## Implementation and verification, 2026-10-12

Migration `0010_twin_persistence.sql` creates the three tables and retention
policies. `twin_core/persistence.py` reads snapshots and parameter overrides,
and moves writes to one bounded background worker. `twin_core/recovery.py`
checks saved JSON and applies the catch-up or settling rule. The runtime restores
each new well before its first tick. The state API now includes `restoredFrom`
and `lastRecoveryEvent`.

The migration applied to the running TimescaleDB volume. The twin role could
insert snapshots and could not update them. On the first service start, all
12 active wells wrote a snapshot and a `cold_start_no_snapshot` event. A service
restart restored all 12, and the database recorded 12 `snapshot_restored`
events. Unit tests cover exact short-gap replay, long-gap settling, rejected
snapshots, future-time hold, and writer replacement. A real host sleep test
has not run; the earlier injectable-clock test covers the jump logic.
After a longer local stop, the live service restored all 12 wells, recorded
about 32 seconds of missing time, and returned to ready.

## Shutdown correction, 2026-10-12

The first implementation queued the final copy on the background writer and
waited only 3 seconds. A slow earlier write could use that entire window,
leaving the final copy unsaved. The shutdown path now writes the final copy
directly after the tick loop stops. It uses the same 3-second connect and
statement timeouts as a periodic write. The older background writer can finish
its current work and queued events during the remaining 3-second join.
Failures of the final write are visible through the snapshot failure metric,
an error log, and a nonzero exit. The last successful periodic snapshot remains
available for recovery if the final write fails.

Docker Compose gives twin-core 15 seconds after SIGTERM before forced stop.
This covers the 3-second connection limit, 3-second statement limit,
3-second background join, HTTP server close, and scheduling margin.
