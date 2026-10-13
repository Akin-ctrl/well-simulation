# ADR 0034: Record An Audit Trail For Actors, Not For Auditors

- Status: Accepted, amended
- Date: 2026-08-29
- Implementation: Implemented

## Context

Nothing in this system records who did anything. There is no audit table and no
`created_by` column on any record. Standards 14 and 17 in
the project coding standards both fail, and a code review does not catch it,
because nothing in the code is wrong. The capability is simply absent.

This matters now rather than later. The feature roadmap adds a control API in
milestones 4 to 8. A command that changes what a wellhead does, with no record
of who sent it, breaks both standards on its first request. The table has to
exist before that work starts, not after.

The standards document names OWASP ASVS and ISA/IEC 62443 as baselines. ASVS V7
requires logging both successful and failed authentication events, and requires
that credentials never appear in a log.

The project is a portfolio piece. It runs on a local stack with three accounts
and made-up telemetry. The audit trail has to be credible to someone who knows
industrial systems. It does not have to survive a hostile auditor, and building
as though it did would be dishonest about what this is.

## Options Considered

### Option A: No audit trail until the control API exists

- Pros: no work now, and the control API is the first thing that truly needs it.
- Cons: leaves two standards failing with no plan. The twin work would then have
  to add auditing while also adding control, which is the point at which
  shortcuts get taken.

### Option B: A minimal audit table covering authentication and user lifecycle

- Pros: closes the gap with a small, reviewable change. Gives the control API
  somewhere to write when it arrives. Enough to demonstrate the judgment.
- Cons: does not cover configuration change history in full, and has no tamper
  evidence.

### Option C: A full audit subsystem

Hash-chained rows, a separate write-only store, alerting on failure patterns,
and a browsing interface.

- Pros: closest to what a regulated plant would run.
- Cons: tamper evidence proves nothing when one person owns the database, the
  application, and the keys. It would be a demonstration of effort rather than
  of judgment, and it is a lot of code to maintain for a simulation.

## Decision

Use **Option B: a minimal audit table covering authentication and user
lifecycle**, shaped so the control API can write to it unchanged.

## What Is Recorded

One `audit_event` table, as a TimescaleDB hypertable so it takes a retention
policy the same way the historian does.

| Column | Purpose |
| --- | --- |
| `occurred_at` | when it happened, and the hypertable time column |
| `actor_user_id` | the user who acted, null when there is no known user |
| `actor_identifier` | the identifier that was submitted, for failed logins |
| `action` | what was attempted, for example `auth.login` |
| `subject_type` | what it was done to, for example `user` |
| `subject_id` | which one |
| `outcome` | `success` or `failure` |
| `request_id` | ties the row to the request and to the logs |
| `detail` | jsonb, for anything action-specific |

Events covered at this stage:

- `auth.login`, success and failure
- `auth.logout`
- `user.create`, `user.disable`, `user.role_change`
- `config.change`, for edits to alarm rules and asset metadata

A request id is generated at the API edge, added to every structured log line,
and stored on the row. That is what turns a row into something investigable: you
can take an audit event and find the request that caused it.

Retention is 90 days.

## Failed Logins

Failed logins are recorded. ASVS V7 requires it, and repeated failures are the
only signal that shows credential stuffing or password spraying.

Three rules go with that:

- Store the submitted identifier only. Nothing from the password field ever
  reaches the table. People do type passwords into the username box.
- Write the row whether or not the account exists. If rows only appeared for
  real accounts, the presence of a row would tell an attacker the account is
  real.
- Rely on the existing rate limit to bound volume. `/auth/*` is capped at 10 a
  minute per client, so an attacker cannot fill the table faster than that.

## What Is Deliberately Left Out

**Tamper evidence.** Hash chaining or an append-only store. The same operator
owns the database, the application, and the keys, so a chain proves nothing
here. It belongs in a deployment where those are separated.

**A separate audit database.** Same reasoning.

**Alerting on failure patterns.** Phase 8 adds metrics. Alerting belongs with
that work, reading from this table, not built into it.

**A browsing interface.** That is dashboard work and can have its own ADR.

**Full draft versus active configuration.** Standard 17 asks for a clear
boundary between draft configuration, active configuration, and history. This
ADR records config changes and who made them, which is the history half. The
promotion workflow, where an edit sits pending until someone activates it, only
earns its place once the control API exists. It is deferred with that reason,
not forgotten.

**How a config change reaches a running service.** That is a separate decision
with its own trade-offs, recorded in ADR 0035. Putting a reload mechanism inside
an ADR about record keeping would hide a behaviour change where nobody would
look for it.

## Rationale

The audit trail exists so a person can answer a question about their own system.
It is built for the actor, not for a regulator.

That framing decides every exclusion above. Anything that only makes sense when
you distrust the operator has been left out, because in this deployment there is
no one else to distrust. Anything that helps someone reconstruct what happened
has been kept.

## Consequences

- Every user-facing mutation gains an audit write, so route handlers get one
  more responsibility.
- The control API has a table to write to on day one.
- A `config.change` row records that someone edited a row. Whether that edit
  reaches the running service is decided in ADR 0035.
- Standards 14 and 17 move from failing to partly met. Standard 17 stays partial
  until the promotion workflow exists.

## What Would Make Us Revisit

- The system is deployed somewhere the operator and the auditor are different
  people. Tamper evidence stops being theatre at that point.
- The control API ships. The promotion workflow should be reconsidered then.
- Audit volume stops being bounded by the rate limiter, for example if a machine
  client is given credentials.

## Amendment 2026-10-03: Name the rate-limited routes exactly

### The problem

This record says "`/auth/*` is capped at 10 a minute per client". The limiter
did match every path under `/auth/`, and that included `/auth/me`, the session
check the dashboard makes. Every timed refresh repeated that check, so one open
tab spent 6 of the 10 requests a minute, and a second tab locked its user out.

### Decision

The strict budget of 10 a minute per client now covers three named routes:
`/auth/login`, `/auth/register`, and `/auth/logout`. `/auth/me` moves to the
general budget of 100 a minute. It checks no password and writes no audit row,
so the bound this record relies on is unchanged.

Logout stays on the strict budget on purpose. It writes an `auth.logout` row,
and it only deletes the cookie without revoking the session, so a copied
cookie could replay it. Moving it to the general budget would let one client
write 100 audit rows a minute. **Do not move `/auth/logout` off the strict
budget** unless logout starts revoking the session.

The list lives in `CREDENTIAL_PATHS` in
`monorepo/apps/api/src/middleware/rate-limiter.ts`. A new route that checks a
password or writes an audit row must be added there.

## Amendment 2026-10-03 (later): Logout now revokes the session

[ADR 0037](0037-session-revocation.md) makes logout end every session of the
user on the server. A copied cookie can no longer replay logout: the second
call is refused before any audit row is written. The reason given above for
keeping `/auth/logout` on the strict budget no longer applies, but the budget
stays as it is. Ten logouts a minute is far above real use, and the limit is a
second line of defence if revocation ever regresses.

## Amendment 2026-10-13: Control and parameter writes require atomic audit

A live control or parameter publication must not succeed without its actor
record. Milestone 8B therefore inserts audit rows in the same transaction as
accepted draft, activation, cancellation, and publication changes. A failed
audit insert rolls back the operation. The existing best-effort helper remains
appropriate for authentication events, where an audit outage must not silently
change the login result.

Twin-core records application, rejection, and expiry in control_command_event
in the same transaction as the command result. The command retains its human
activation identity. This costs a database dependency for new control changes,
but allows every accepted change to be traced after a restart. See
[ADR 0038](0038-live-control-command-lifecycle.md).
