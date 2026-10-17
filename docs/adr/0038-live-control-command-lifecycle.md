# ADR-0038: Apply live model controls through durable, reviewed commands

**Status**: Accepted
**Implementation**: Built; verification results are recorded in the Milestone 8B design.
**Date**: 2026-10-13
**Decision**: An operator creates a draft command, then explicitly activates it. Activation records the command and its audit event before twin-core applies the target controls at a model tick. A command expires after 30 seconds if it cannot be applied. The active control row has a revision, so a stale command cannot replace newer controls. This changes only the synthetic model, never a physical actuator.

Roadmap: Milestone 8B in the local feature roadmap.

---

## Context

Twin-core holds each well's controls in memory. A restart recovers them from a model snapshot. Forecasts and scenarios copy those controls but never change the running well. Milestone 8B needs a separate path for a deliberate live change.

A direct HTTP request that changes memory first can be lost on a crash. A database row alone cannot tell the caller when the running model applied it. The existing audit helper may swallow a failed write, which is acceptable for a login but not for a control change. The public API also uses the database administrator account in Compose today. That credential must be reduced before control commands are enabled.

This project has no physical control connection. The Modbus gateway publishes model telemetry; it does not accept control writes. The emergency shutdown field is a synthetic model input, not a safety instrumented function.

## Options considered

### A. Apply through HTTP, then save

The API could send controls directly to twin-core and save the result afterwards. It is small and fast. A crash between the memory change and database write would leave an unattributed or unrepeatable change.

### B. Save a command, then apply at a tick (chosen)

The API saves an activation request and audit row together. Twin-core receives an HTTP wake-up and also scans for queued commands after a missed wake-up or restart. It applies only a current command at a tick. This adds a small queue and a recovery path, but keeps the user's accepted request durable.

### C. Use the database as the only command channel

Twin-core could poll command rows without an HTTP wake-up. This is simple to deploy and survives restarts. It gives normal command latency to the poll interval and turns the database into the sole service interaction path, contrary to ADR 0011.

## Decision

### Command states and data

Migration 0012 adds three control tables and the separate model_parameter_change
table. These are ordinary PostgreSQL records rather than telemetry hypertables.

- control_command stores one immutable proposal: command UUID, well ID, creator, reason, five optional control targets, expected control revision, model version, parameter fingerprint, creation time, and idempotency key. Its status moves through draft, queued, then applied, rejected, expired, or cancelled. It also records activation and result times.
- wellhead_control_state stores one current row per well: a nonnegative revision, all six model control values, the current model version and parameter fingerprint, the applied command UUID, and the time they became active. The swab valve remains a stored model value but is not an editable command target.
- control_command_event is append-only. It records each state change, its time, command ID, actor when human, and a bounded reason code. Command and event rows have no automatic retention in this local portfolio deployment.

Database checks enforce choke position from 0 to 100 percent, at least one proposed field, and valid statuses. Unique indexes allow one draft per creator and well, and one queued command per well. The API can update only command lifecycle fields. Twin-core can read queued commands and update only their result fields and the active control row. Neither service can delete command history. The migrator keeps schema ownership.

Drafts do nothing to the model. A changed draft is a new command; the old one is cancelled. Only its creator may activate or cancel a draft. That operator performs both steps. This is explicit confirmation, not two-person approval. A draft older than 24 hours expires without affecting the model.

### Authorisation and activation

The public API requires the existing control:issue capability for draft, activate, cancel, and detailed status routes. It also locks and reads the user's current role and status from the database inside each write transaction. Role changes and command writes therefore have a definite order. A signed token carrying an old OPERATIONS role cannot activate after the account is disabled or downgraded. ADMIN does not inherit control authority.

Activation first checks that twin-core has a current running model. It then locks the command and active control row. It requires a draft, the expected revision, the same model version and parameter fingerprint, and no other queued command for that well. It records the expected revision, sets a database-clock expiry 30 seconds away, and inserts an audit_event row in the same transaction. If that transaction fails, no command is queued. The existing best-effort audit helper is not used for a command write.

The API returns queued, never applied, after a confirmed commit. It sends the command UUID over authenticated internal HTTP as a wake-up hint. If the hint fails, a bounded background scan finds the committed row. The command UUID makes repeated activation calls idempotent. Draft creation uses a caller-supplied idempotency key with a unique database constraint. If an activation reply is lost, the caller reads the command status or retries activation. The API does not claim that no command was queued.

### Applying and recovering

Before each one-second fleet step, twin-core may take one prepared command across the whole fleet. It checks the command again against the current state, revision, model version, parameter fingerprint, expiry, and allowed control fields. It commits the new active control row, command result, and command event in one bounded transaction. Only after that commit does it replace the in-memory control set. A failed database write leaves the old controls in force.

The apply transaction locks the activating user's row first. It requires an
active, undeleted account and the same role and session version that the API
verified at activation. Otherwise the result is rejected with
`authority_revoked`. The capability matrix remains in the API; twin-core checks
that the granted authority still exists. Logout or downgrade before application
therefore prevents the queued command from taking effect. Revocation after a
completed application does not undo the active controls. Every command write
uses the lock order user, command, then active control row.

On startup, twin-core loads the active control row before its first model tick. That row always wins over controls in the last snapshot. A difference is recorded as a recovery event. A crash after the database commit but before the next model step therefore does not lose an active setpoint. Applied means the model's active control target changed; it does not promise a completed process step or any physical action.

An expired, stale, invalid, or conflicted command becomes a terminal result with a reason. It is never retried by making a fresh command silently. A queued command may be cancelled while it remains queued. Row locking decides whether cancellation or application wins. A terminal command cannot move again. If the result of an apply commit is unknown, twin-core holds that well unavailable until it reads the active row again. It never assumes the old controls are still active.

### Separate parameter changes

An ADMIN uses the same authenticated identity, review, and transactional audit pattern for model parameter overrides. The parameter path has its own draft and publish states. Publication writes the existing append-only wellhead_model_parameter record and reports restart required. It does not use the live control queue or change model parameters during a tick. Twin-core remains the authority for parameter ranges and units.

### Implementation gates

Milestone 8B replaces the API's Compose database administrator credential with a scoped application role. The Drizzle mirror and both OpenAPI files cover the implemented tables and endpoints. It cannot claim a physical control safety rating. A real deployment would need a site risk assessment, a separate safety system, and device-specific command verification.

## Consequences

**Good.**

- A user can distinguish draft, queued, and active controls.
- A crash or missed HTTP wake-up does not silently discard an accepted command.
- A stale or repeated request cannot override a newer active revision.
- The command record names the human actor and the model state it expected.

**Costs.**

- The API and twin-core share a small command lifecycle across HTTP and PostgreSQL.
- A control application adds one bounded database transaction near a model tick.
- Startup must reconcile the active control row with the last state snapshot.
- Existing installations need separate API database credentials before upgrading.

**Accepted risks.**

- The database can commit an active setpoint just before the process exits. The next process restores that setpoint; the intervening model tick is a visible gap.
- A database outage stops new control changes, while the model may continue with its last active controls. The control status must show the outage.
- A database transaction may commit just before its reply is lost. The command UUID and active revision make reconciliation possible, but the affected well stops ticking until that check completes.
- The 30-second expiry can reject a command during a restart. The operator must review and submit a new draft, so an old command never surprises them later.
- One operator can activate their own draft. This is suitable for a synthetic portfolio system, not a claim of independent approval.

## Amendment, 2026-10-17: operating screens and outcome recovery

The owner requested live control and parameter screens after the dashboard
usability work. The [operating workspace design](../design/operating-workspace.md)
keeps the existing draft, review, activation and publication boundaries.
OPERATIONS uses Controls. ADMIN uses Parameters and does not gain control authority.

Bounded per-well history and exact own-key lookup let a user recover a missing
response without repeating a write. These reads recheck current account authority
and work without the model. Python supplies parameter definitions and running
values, so the frontend does not maintain another list of units or bounds.
The screens distinguish database acceptance, running adoption and restart-only
parameter changes. The single SQL schema needs no change for these additions.

## Related

- [Milestone 8B design](../design/twin-core-milestone-8b.md): implementation, operating steps, and verification results.

- [ADR 0007](0007-control-and-what-if-boundaries.md): scenarios remain isolated from live controls.
- [ADR 0015](0015-control-and-safety.md): names the five editable control fields.
- [ADR 0011](0011-twin-core-communication.md): uses internal HTTP plus durable storage.
- [ADR 0021](0021-model-parameterization.md): parameter overrides require a distinct publish path.
- [ADR 0033](0033-role-model-and-access-control.md): OPERATIONS may issue control; ADMIN cannot.
- [ADR 0034](0034-audit-trail-scope.md): supplies the audit table, with stricter control-write behavior here.
- [NIST SP 800-82 Rev. 3](https://csrc.nist.gov/pubs/sp/800/82/r3/final): OT security guidance on access control and availability.
- [ISA/IEC 62443 series](https://www.isa.org/standards-and-publications/isa-standards/isa-iec-62443-series-of-standards): industrial security lifecycle and risk framing.
