# ADR 0033: Separate Operating Authority From Administrative Authority

- Status: Accepted
- Date: 2026-07-31

## Context

The database defines a `role` enum with `USER`, `ADMIN`, and `OPERATIONS`. The API
stamps the value into every session token. **No code reads it.** There is no
authorization guard anywhere in the repository, so every authenticated account has
identical, complete access to all fleet telemetry and alarms.

Registration compounds this: `POST /auth/register` is public and unauthenticated, so
anyone who can reach the dashboard can create an account and read the whole fleet.

`CODING_STANDARDS_COMMITMENT.md` Standard 12 requires that "roles and permissions must
be explicit, meaningful, and enforceable" and that "administrative power must not be the
default fallback for ordinary users or services". The current state satisfies none of
that, and the standards document aligns the repository with ISA/IEC 62443, which treats
operating a plant and administering a system as distinct authorities.

This decision cannot wait for the twin. Milestones 4 through 8 introduce a control API
that accepts choke position, valve state, and pump state. Whatever role model exists when
that lands becomes the model that gates physical actuation.

Related decisions: ADR 0007 separates live control from what-if scenarios, ADR 0014 runs
scenarios against isolated cloned state, and ADR 0015 restricts live control to
actuator-like variables.

## Options Considered

### Option A: Hierarchical roles, ADMIN as a superset

`ADMIN ⊃ OPERATIONS ⊃ USER`. One role grants everything.

- Pros: Simplest to implement and reason about; conventional for web applications; a
  single role to grant when someone "needs everything".
- Cons: The account that manages users and edits alarm thresholds can also open a choke.
  Administrative compromise becomes plant compromise. Directly contradicts Standard 12's
  prohibition on administrative power as the default fallback.

### Option B: Separate operating authority from administrative authority

`USER` reads. `OPERATIONS` acts on the plant. `ADMIN` administers the system. Neither
`OPERATIONS` nor `ADMIN` inherits the other.

- Pros: Matches ISA/IEC 62443 expectations and Standard 12. Compromising the account that
  manages users does not grant actuation. Makes "who can move a valve" answerable by
  looking at one column.
- Cons: An operator who is also an administrator needs both grants. Slightly more work to
  implement than a single ordering.

### Option C: Fine-grained permissions instead of roles

Replace the enum with a permission set per user.

- Pros: Most flexible; no role redefinition needed as capabilities grow.
- Cons: Premature. The system has three meaningful actor kinds and no requirement that
  distinguishes users within them. Adds a permission-management surface, and its own
  administration problem, before there is anything to manage.

## Decision

Use **Option B: separate operating authority from administrative authority**.

`ADMIN` does **not** inherit control authority. An administrator who must operate the
plant is granted `OPERATIONS` explicitly, and that grant is an audited event.

## Permission Model

`USER` is the baseline every authenticated account holds. `OPERATIONS` and `ADMIN` each
add to it along a different axis.

| Capability | USER | OPERATIONS | ADMIN |
| --- | :---: | :---: | :---: |
| View dashboard, trends, wellhead detail | yes | yes | yes |
| View active alarms and threshold context | yes | yes | yes |
| Run what-if scenarios | yes | yes | yes |
| Acknowledge or shelve an alarm | no | yes | no |
| Issue a live control command | no | yes | no |
| Create, disable, or re-role a user | no | no | yes |
| Edit alarm rules and thresholds | no | no | yes |
| Edit asset and parameter metadata | no | no | yes |

### Why scenarios are open to every role

ADR 0014 runs what-if scenarios against isolated cloned state with no path to the live
model and no actuator effect. They are an analysis feature, not a control action, so
restricting them would buy no safety while making the system less useful to the audience
it is meant to explain itself to. Scenario runs are still attributed to an actor.

### Registration

Self-service registration is removed. `POST /auth/register` requires an authenticated
`ADMIN` session.

The seed fixture creates one read-only `USER` account for demonstration, with credentials
documented in the README quick start, and one `ADMIN` bootstrap account whose password is
generated and logged once by the migrator on first run.

This keeps the project reviewable in five minutes without leaving an unauthenticated
write endpoint on an operations dashboard.

## Rationale

The distinction this encodes is the one an industrial reviewer will look for: the person
who administers the software is not automatically the person permitted to move equipment.
Encoding it costs one guard and a documented matrix, and it means the control API arriving
in Milestones 4 through 8 is gated by a model that was designed before it, rather than
retrofitted to whatever the code happened to do.

Choosing three roles over a permission system keeps the model explainable. If a
requirement appears that two accounts with the same role must differ, that is the signal
to revisit.

## Consequences

- Role-guard middleware must exist and be applied per route. Until it does, this ADR
  describes intent rather than behaviour.
- Every guarded route needs a test asserting the roles that must be rejected, not only the
  role that is accepted.
- `OPERATIONS` has no capabilities to guard until alarm acknowledgement and the control
  API exist. The role is defined now so those features are built against it.
- Granting or changing a role is a security-significant action and must be attributable.
  This creates a dependency on the audit trail in the remediation roadmap's Phase 5; until
  that lands, role changes are enforceable but not traceable.
- The public OpenAPI contract must show `/auth/register` as requiring authorization.
- The demo account's existence and its read-only role must be stated in the README, so a
  reviewer does not mistake it for a security lapse.

## What Would Make Us Revisit

- A requirement that two accounts sharing a role need different capabilities, which would
  favour Option C.
- Multi-tenant or multi-field deployment, where authority needs to be scoped to an asset
  subset rather than granted globally.
- A regulatory obligation requiring formal approval workflows, where issuing a control
  command needs a second actor rather than a single role check.
