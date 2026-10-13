# Milestone 8B: Live controls for the synthetic well

Status: implemented and verified locally, 2026-10-13.

## Goal and boundary

An OPERATIONS user can ask the running synthetic well to use a new choke, pump,
valve, or emergency shutdown input. The request has a draft step and an explicit
activation step. A response says whether the command is drafted, queued,
applied, rejected, expired, or cancelled. It never calls a queued command
applied.

This milestone changes the in-memory model and its durable active control row.
It does not write a Modbus coil or register. The current Modbus gateway reads
the model snapshot and publishes telemetry. Its status block at holding
register offsets 1900 to 1904 carries the model tick and version, not a
command. No wire frame or device register changes in 8B. Emergency shutdown
remains a model input; it is not a real well safety system.

[ADR 0038](../adr/0038-live-control-command-lifecycle.md) records the command
decision. [ADR 0007](../adr/0007-control-and-what-if-boundaries.md) keeps live
controls separate from private scenarios. [ADR 0015](../adr/0015-control-and-safety.md)
names the five editable fields.

## Starting point before 8B

- ModelRegistry owns one ControlInputs value per active well under its lock.
  Its tick method advances each well in one-second steps.
- A saved snapshot contains process state, including control values. Startup
  restores those values. The snapshot writer runs every 30 seconds.
- The API checks a signed session and the control:issue capability, but its
  role claim can outlive a role change until the user signs in again.
- The API's audit helper logs and swallows a failed audit insert. This is too
  weak for a command that changes the active model.
- The Compose API uses the PostgreSQL administrator account. It must use a
  scoped application account before this route is enabled.
- Forecast and scenario requests already use a separate internal service token.
  Their routes do not change active controls.

## Data flow

1. The API checks the session and reads the user's current role and account
   status from the database. Only an active OPERATIONS user may create,
   activate, or cancel a control command.
2. Draft creation stores one proposal and a matching audit row in a transaction.
   A draft has no model effect.
3. Activation requires a current running twin-core model, then locks the draft
   and active control row. It compares the control
   revision, model version, and parameter fingerprint. It stores queued status,
   expiry, and audit in one transaction, then returns HTTP 202.
4. The API sends only the command UUID to twin-core over authenticated internal
   HTTP. This is a wake-up hint. Twin-core also scans the durable queue once a
   second, so losing the hint cannot lose the command.
5. Before a model tick, twin-core rechecks the queued command. A bounded
   transaction moves the full target controls into the active row and records
   applied status and an event. It then installs the same controls in memory
   under the registry lock.
6. The model takes the next step with those controls. The API's status read
   shows the durable result. The later dashboard may display it, but a new
   dashboard control form is outside this milestone.

The API owns human identity and draft activation. Twin-core owns model
validation and the point of application. PostgreSQL owns the accepted command
and active revision. Modbus remains a read path.

## Control logic and units

The command is a set of absolute targets for supplied fields. Missing fields
keep their current values. JSON null is invalid; it cannot mean unchanged.

| API field | Model field | Unit and allowed values |
| --- | --- | --- |
| chokePositionPercent | choke_position_percent | Percent, finite 0 to 100 |
| masterValveOpen | master_valve_open | Boolean |
| wingValveOpen | wing_valve_open | Boolean |
| pumpOn | pump_on | Boolean |
| emergencyShutdown | emergency_shutdown | Boolean |

Swab valve is part of the full model control state but cannot be changed by a
command. Pressure, flow, temperature, gas, and damage are model outcomes and
cannot be targets.

For a command applied before tick k+1:

    u[k+1] = merge(u[k], c)
    x[k+1] = F(x[k], u[k+1], p, t[k+1])

Here u is the full ControlInputs value. c is the validated command target,
using percent for choke and booleans for the other fields. x is ProcessState,
including pressure in psi, flow in barrels per day, and temperature in degrees
Fahrenheit. p is the fixed ModelParameters value for that well. t is the UTC
tick time. F is the existing step_model function, with a one-second step.
The command does not edit x directly. If the command reaches the worker just
after a tick starts, it waits for the next tick.

An emergencyShutdown true command contains no other target field. While that
input is true, other control changes are refused. Reset is a separate command
whose only target is emergencyShutdown false. This rule avoids hiding a valve
opening inside a reset. It does not replace a physical emergency shutdown.

## Schema and state machine

Migration 0012 adds ordinary tables. SQL migrations remain canonical, and
Drizzle mirrors their shape.

| Table | Important columns and rules |
| --- | --- |
| control_command | UUID key; well and creator foreign keys; reason; five nullable typed target columns; expected_revision; model_version; parameter_fingerprint; UUID idempotency_key; status; created, queued, expiry, and result times; activated_by |
| wellhead_control_state | One row per well; revision; all six typed controls; model_version and parameter_fingerprint; applied_command_id; applied_at |
| control_command_event | Append-only event ID; command ID; event type; time; human actor when present; bounded reason code |

A unique constraint on creator plus idempotency key prevents a repeated draft
request from making two commands. A retry with the same key and different
body returns 409. Partial unique indexes allow one draft per creator and well,
and at most one
queued command per well. A stale expected revision returns 409. Database checks
enforce choke bounds and at least one target. No command table has automatic
retention; these rows are the operational history after the 90-day audit-event
retention window.

    draft  -> queued     -> applied
       |        |         -> rejected
       |        |         -> expired
       |        |         -> cancelled
       +------> cancelled
       +------> expired

Terminal states never return to queued. A draft is immutable. Only its creator
may activate or cancel it. A draft expires after 24 hours. To change one,
cancel it and create another. Repeating activation for the same command returns
its current state and does not allocate another revision. A queued command
expires 30 seconds after activation, using the database clock. A rejected or
expired command requires a new draft.

Applied records a committed active setpoint. Twin-core then installs that same
revision in memory before stepping the model. A status read can see the database
commit just before memory installation; it exposes both revisions separately.
The next completed model tick may occur later.
It does not mean a physical device moved.

## HTTP contract

The API uses its normal response envelope. The internal service uses its
existing token. The public and internal OpenAPI files describe these routes.

| Route | Actor | Result |
| --- | --- | --- |
| GET /twin/wellheads/{id}/controls | Signed-in reader | Active values, revision, model status, pending command ID |
| POST /twin/control-commands | Current OPERATIONS user | 201 draft; requires Idempotency-Key, well ID, expected revision, reason, and one or more targets |
| POST /twin/control-commands/{id}/activate | Current OPERATIONS user | 202 queued; explicit confirm true and expected revision |
| POST /twin/control-commands/{id}/cancel | Current OPERATIONS user | 200 cancelled if still draft or queued |
| GET /twin/control-commands/{id} | Current OPERATIONS user | Durable status, timestamps, actor, and safe reason code |
| POST /control-commands/{id}/notify | API service only | 202 wake-up accepted; no command payload |

The internal wake-up never serves as proof that a command applied. The public
create and activate routes have a strict per-user limit of ten requests per
minute, plus one queued command per well. A 409 means a stale revision or state
conflict. A 503 means the database or model cannot accept work. Missing
authentication returns 401, wrong authority 403, bad data 400, and an unknown
well or command 404. A cancelled or expired command is visible through GET.

The API locks the user row and checks current database role, status, deleted
flag, and session version in each write transaction. The role check uses the
existing capability matrix, rather than a second role list. A concurrent role
change must finish before or after that transaction. The existing JWT role
guard stays as an early check, but cannot be the final authority for
actuation. A newly promoted operator may need to sign in again before the
early guard allows a draft. A downgraded operator cannot activate with an old
token.

Accepted draft, activation, and cancellation writes put audit_event in the
same transaction as the command change. A failed audit insert rolls back the
change. Rejected requests produce a safe security log and a best-effort audit
row where possible. No raw token or secret enters either record.

## Tick, restart, and failure logic

The inbox worker reads at most 100 queued rows per scan and wakes once a second
or on an HTTP hint. It keeps at most one prepared command per well. A backlog
above 100 is exposed through the control backlog metric; it is never silently dropped. The
model tick performs no queue scan. It applies at most one prepared command
across the whole fleet per tick, giving older queued commands a fair turn.

Lifecycle rows and transactional audit events record drafts and activation.
Worker metrics count application outcomes, expiry, and reconciliation by reason.
They also show queue depth, the backlog beyond one scan, inbox readiness, and
apply duration.
Logs carry the command UUID, well ID, and request ID where available, but no
token or raw reason text.

The apply transaction uses row locks, a 75 ms lock timeout, and a 250 ms
statement timeout. Opening a connection has a separate two-second timeout.
These are individual operation limits, not a guaranteed 250 ms total deadline. The application checks expiry
using the database clock. A slow call may still delay a tick; the scheduler
must count that gap rather than pretend it ran on time.
It locks the activating user's row before the command and active control rows.
It rechecks the current account status, deleted flag, role, and session version.
Activation stores the verified role and session version with the command. A
change to either, or an inactive account, rejects the command with
`authority_revoked`. Python compares that recorded authority with the current
row; the API's shared capability matrix remains the authority for granting it.
Logout therefore cancels authority for a queued command too. Revocation that
commits after the apply transaction cannot undo an already applied setpoint.

The transaction checks queued status, expiry, expected revision, active model
version, and parameter fingerprint again. A timeout leaves the old controls active and the
command queued until the next try or expiry. The command, active row, and event
commit together. Only then does the registry change its in-memory controls.
If an unexpected error prevents that memory change, the well becomes
unavailable and logs the command ID; restart reloads the active row. If the
commit outcome is unknown after a lost database reply, the well also becomes
unavailable until twin-core re-reads the active row and command status. It
does not guess whether the old or new controls are active.

On startup, twin-core loads active controls before the first tick. For a new
well with no control row, it creates revision zero from a validated snapshot or
the documented defaults. An existing active row always wins over controls in
the snapshot. If they differ, recovery keeps the saved physical values and moves their time
to the recovery instant. It records the missing seconds and a control recovery
event. It does not settle the gap using obsolete inputs. The next ordinary
one-second step uses the durable controls. A model version or parameter change updates
the active row's model identity before accepting new drafts, without changing
its control revision. Startup expires unlocked queued commands before releasing
new wells. A concurrently locked row is checked again during application and
by the next inbox scan. An expired command never applies after an outage.

| Failure | Result visible to the caller |
| --- | --- |
| Twin-core offline after activation | Queued until recovery, then applied or expired; never falsely marked applied |
| Database unavailable before activation commits | 503; no command or audit row committed |
| Activation commit reply lost | Outcome unknown; retry with the same idempotency key or read status before creating anything new |
| Database unavailable before apply commits | Old controls remain active; command stays queued until retry or expiry |
| Apply commit reply lost | Well held unavailable until active revision is read back; no guessed control state |
| Duplicate activation or HTTP hint | Same command ID and revision; no second application |
| Changed model or parameters | Command rejected with a safe reason; old controls remain |
| Logout, role change, or disabled account before application | Rejected with authority_revoked; old controls remain |
| Invalid or out-of-range target | 400 at the API; twin-core also rejects it before apply |
| Crash after active-row commit | Restart loads that active row; skipped process time stays visible |

## Parameter overrides use the same write trust boundary

Milestone 8B also retires hand-entered SQL for per-well parameter changes.
ADMIN owns this path; OPERATIONS control authority does not grant it. It uses
a separate model_parameter_change draft table, with key, value, unit, reason,
model version, expected latest override ID, and an idempotency key. Proposals
expire after 24 hours. Only their creator may publish or cancel them. Drafts do not alter the
running model.

Twin-core provides a read-only validation endpoint using the existing
parameter resolver. Publication validates again, checks the expected override
ID and current ADMIN role, and inserts one append-only
wellhead_model_parameter row with an audit row in one transaction. Publication
returns published, restart required. It never says the new parameter is active
in the running model. The service is told to refresh, and its existing
restart_required state remains authoritative. If that notification fails, the
next 30-second fleet refresh finds the row. The API reports notification
pending rather than claiming an immediate runtime change.

The parameter proposal has its own status read and cancel route. It does not
enter control_command, use the 30-second control expiry, or change the model
inside a tick. The API shows the published record and the currently running parameter
fingerprint separately until restart completes. A well with an invalid cold
configuration has no running fingerprint; validation can still repair its
parameter set.

## Build and verification order

1. Add canonical SQL migration 0012, the Drizzle mirror, and narrow database
   grants. Move the API away from the PostgreSQL administrator credential.
2. Add typed command and parameter-change contracts. Keep model limits
   authoritative in Python; test the public and internal boundary values
   against each other.
3. Add API draft, activation, cancel, read, and transactional audit paths.
   Test USER, OPERATIONS, ADMIN, disabled, and downgraded accounts.
4. Add the twin-core inbox, tick application, active-row recovery, and metrics.
   Test logout and role revocation while queued, concurrent activation, expiry, duplicate hints, database timeout,
   crash after commit, and snapshot reconciliation.
5. Add the parameter draft and publish path, with a real restart test.
6. Update both OpenAPI files and operational documentation. Verify the
   complete flow against a disposable Timescale database and the running
   Compose stack. Confirm Modbus ingestion still reads model output and that
   no Modbus write function is called.

The release gate is stronger than a green unit suite. A queued command must
never be reported as applied; a failed audit write must prevent activation;
and a restart must recover the last active controls without replaying an
expired command. A real plant deployment remains outside this project's
claim and would require device-specific verification and safety review.

## Implementation map

- data/sql/migrations/0012_live_model_controls.sql owns the tables, immutable
  proposal checks, lifecycle transitions, and narrow row-lock helpers.
- data/src/schema.py provisions separate twin and API logins. The API can
  write proposals and audit rows but cannot update the active control table.
  Twin-core cannot edit user accounts or delete command history.
- monorepo/packages/dto/req/controls.ts owns the public request shapes.
  Scenarios and live controls share the allowed field schema; live shutdown
  commands add the separate-reset rule.
- monorepo/apps/api/src/control-service.ts owns account checks, locking,
  idempotency, and transactions. twin-control-client.ts owns internal HTTP and
  response validation. routers/controls.ts owns public HTTP and rate limits.
- twin_core/control_store.py owns database transactions. controls.py owns the
  bounded inbox. state.py installs confirmed controls under the registry lock.
  runtime.py connects recovery, validation, refresh, and the next model step.

## Operating the API

For an existing local stack, add API_DB_USER and a separate URL-safe
API_DB_PASSWORD to .env, then run docker compose up -d --build. The migrator
keeps schema ownership and provisions both application logins. Preserve the
existing volume; this is an additive migration.

An administrator creates an OPERATIONS account through the existing user API.
That operator signs in and reads GET /api/twin/wellheads/1/controls. Use the
returned active revision when creating a draft:

```json
{
  "wellheadId": 1,
  "expectedRevision": 0,
  "reason": "Demonstrate a lower synthetic choke opening",
  "targets": {"chokePositionPercent": 50}
}
```

Send it to POST /api/twin/control-commands with a new UUID in Idempotency-Key.
Review the saved draft, then POST {"confirm":true,"expectedRevision":0} to
/api/twin/control-commands/{commandId}/activate. HTTP 202 means queued. Read
/api/twin/control-commands/{commandId} until the status is terminal. Keep the
same command ID after a lost reply; do not create another proposal just to retry.

For parameter changes, ADMIN first reads GET /api/twin/wellheads/1/parameters
to find the current model version and latest override ID for the chosen key.
ADMIN then uses POST /api/twin/parameter-changes with its own
Idempotency-Key. Supply wellheadId, modelVersion, parameterKey, value, unit,
expectedOverrideId (null when none exists), and reason. Publish using
POST /api/twin/parameter-changes/{changeId}/publish with {"confirm":true}.
A null value and null unit together remove the override. The status read
shows the published row ID alongside the running model's parameter fingerprint.
Restart twin-core after reviewing the publication. The restart restores active
controls and loads the newly validated parameter set.

The API uses same-origin session cookies. Scripted callers must preserve the
authentication cookie and provide the Origin header required by the API's
existing request checks. Neither the browser nor a human caller receives
TWIN_INTERNAL_TOKEN.

## Verification on 2026-10-13

The complete Python suite passed 237 tests before the final expiry and repair
cases were added; its optional projection database test was skipped then.
That projection test subsequently passed against the scoped twin role on a
real disposable Timescale database. The final control database and HTTP group
passed all 24 tests, including the two added cases.
The final control, runtime, persistence, and state regression group passed
another 38 tests after the startup-expiry changes.

The full API suite passed 89 tests while using the restricted API database role.
The control route suite was also rerun after the internal client was separated.
All 11 control route tests passed. The final API image built and became healthy.
The DTO and existing dashboard suites passed 28 tests. Ruff, strict mypy,
TypeScript checks, ESLint, OpenAPI validation, and prose checks form the static
gates alongside these behavior tests.

An initial container build found an undeclared direct Zod import in the API;
the route now imports its shared DTO schema. TypeScript also caught untyped
JSON responses in the new tests; those now use explicit response types. Both
gates passed after correction. A Docker image lookup briefly failed with a
TLS timeout and succeeded on retry. The optional projection database check
was run separately, as described above; it is not counted as a skipped release
gate.

A separate Compose stack first ran the committed Milestone 8 build from a fresh
volume. It was then upgraded with migration 0012 and the working 8B code. The
migrator exited successfully and every long-running service became healthy.
The end-to-end check confirmed:

- An OPERATIONS draft returns 201, activation returns 202 queued, and the
  durable result becomes applied with exactly one new revision.
- Setting choke to 50 percent reaches the historian through Modbus. Model
  flow falls and tubing pressure rises.
- A private scenario leaves the active controls and revision unchanged.
- Restart restores the saved control revision and target.
- ADMIN publication marks the running well unavailable pending restart. The
  restart adopts the new parameter fingerprint and preserves live controls.
- The four existing dashboard pages render with no browser errors on both the
  original and upgraded stacks.

Failure tests cover authority revocation after queueing, duplicate activation,
invalid targets, failed audit or event writes, expiry, changed model identity,
and a real committed transaction whose reply is deliberately lost. The latter
checks that recovery reads the committed controls instead of guessing.

The checks above used an isolated deployment before upgrading the original
local stack. They establish the synthetic command workflow, not field
calibration or a physical control safety rating.
