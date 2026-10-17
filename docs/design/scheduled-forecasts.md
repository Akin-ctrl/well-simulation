# Scheduled forecasts

Date: 2026-10-17
Status: Implemented and verified locally
Related decisions: [ADR 0013](../adr/0013-prediction-strategy.md),
[ADR 0033](../adr/0033-role-model-and-access-control.md)

## Purpose

Keep a recent short forecast available without asking someone to press Run.
The Forecast page keeps its on-demand form and gains a scheduled-forecast panel.
Readers can see whether the schedule is enabled, the next due time, the latest
attempt and the latest completed result. Administrators can create or change the
schedule. New installations start with every schedule disabled.

The panel names the result's creation time and age. A failed or overdue attempt
does not make an older result current. Opening the result reads its existing URL;
it never calculates another forecast. A result records whether a person requested
it or a schedule created it. Saved runs show that distinction too.

## Configuration and authority

One schedule belongs to one well. It has an owner, enabled flag, interval, forecast
duration, revision, next due time, update time and reason. Administrators use the
existing `asset:manage` capability to edit it; all dashboard readers can inspect
it. The editor must confirm the reviewed settings and supply a reason. A revision
check rejects changes made against an older copy. An uncertain write leads to a
GET status check, without automatic replay.

Intervals are whole minutes from 5 to 1440; duration is 5–30 whole minutes. The
worker saves a chart point every 60 seconds. Defaults are a 15-minute interval
and a 15-minute duration. Saving an enabled schedule starts a new interval from
the database clock. Disabling stops future claims; a calculation already in
progress may finish. The page explains both effects before submission.

The administrator who saves the settings becomes their owner. A schedule persists
through logout, but the worker rechecks the owner's active ADMIN authority before
claiming and before saving a result. Revoked authority disables future work and
records the reason. The existing narrow `control_actor_authority` SQL function
already provides a locked authority read without exposing password columns.
Its name remains for compatibility; its returned role/status values support this
check. The service never borrows a browser token or invents a human click.

## Durable records

Add ordinary tables to `data/sql/schema.sql`:

- `forecast_schedule`: UUID key, unique well, owner, settings, revision, due time,
  reason and update time.
- `forecast_job`: UUID key, schedule/revision, copied owner/well/settings,
  scheduled time, start time, lease expiry, missed-interval count, status,
  completion time, reason code and optional completed prediction run UUID.

A job is unique by schedule and scheduled time. A partial unique index allows
only one running job per schedule. Checks bound all settings and require a run
identifier exactly when status is complete. Terminal jobs have completion times;
running jobs do not. The public API can read jobs but cannot rewrite their results.
The model worker can advance due times, claim and finish jobs. Neither new path
can write live controls.

A result still uses `prediction_run` and `prediction_point`. Its existing base-state
JSON gains `executionOrigin`, with scheduled job, schedule and due-time identity.
The job copies the settings used even if an administrator later changes the
schedule. The result keeps its full state, parameters, controls and alarm rules.
Legacy results without this metadata are manual, their only earlier source.
Run attribution identifies the schedule owner; execution metadata identifies the
worker action. The central audit records settings changes; attributed job records retain worker
outcomes. The worker receives no general audit-table or user-table read grant.

## Claim and execution logic

The scheduler runs in its own thread, outside the one-second model tick loop.
It checks for work every second and processes one job at a time. It shares the
existing two-slot projection capacity with on-demand requests. Readiness failures,
capacity exhaustion and calculation errors produce explicit job outcomes.
They do not retry the same scheduled job automatically.

Read one candidate identity, then use a short transaction to lock its owner
first and its still-matching schedule second with `FOR UPDATE SKIP LOCKED`.
The owner-first order matches API writes and avoids a user/schedule deadlock.
Recheck owner identity, enabled state and due time under the schedule lock. PostgreSQL recommends this behavior for queue consumers; it avoids
waiting on a row another worker already claimed. Queries use stable ordering,
a limit of one and the existing database statement/lock timeouts.

Let `n` be the database clock, `d` the stored `next_due_at`, and `i` the schedule's
`interval_seconds`. If `n >= d`, calculate:

- `m = floor((n - d) / i)`, the missed interval count.
- `scheduled_for = d + m * i`, the most recent due slot.
- `next_due_at = scheduled_for + i`, the first future slot.

All durations above use seconds; stored instants use UTC. The claim stores `m`
with one job and advances the schedule in the same transaction. There is no loop
that reconstructs every missed interval. A backwards clock movement leaves
future due times in place until wall time catches up.

A claim lasts 60 seconds. The worker copies fresh running state, keeps current
controls, and calculates the configured bounded forecast. Saving run metadata,
all points and job completion happens in one transaction. Before inserting, lock
the owner first, then the job, and recheck its running state, unexpired lease
and owner authority. The
job UUID is the claim identity and is never reassigned to another attempt.

If the process dies after claiming, a bounded cleanup marks expired running jobs
failed. It never replays them. A worker that finishes after expiry cannot commit a
result. If commit succeeds but the response is lost, the job and complete result
already agree; cleanup will not rewrite them. Schedule edits affect later claims;
a claimed job retains its copied settings.

## API and UI

`GET /twin/wellheads/{id}/forecast-schedule` returns the current schedule or null,
the latest job and the latest completed scheduled result summary. Queries return
only metadata. `PUT` accepts expected revision, enabled flag, interval/duration,
reason and explicit confirmation. The server rechecks authority inside the write
transaction. Conflict returns 409; uncertain dependency failures return 503.
No automatic write retry occurs.

The panel shows Disabled, Scheduled, Running, Overdue or Attention required using
text. It reports missing results separately from a failed latest attempt. History
and saved result headings identify Scheduled forecast. The administrator editor
sits in an expandable section. Readers see an explanation of who manages settings
without a disabled form filling the page.

Mockups cover reader and administrator views, disabled/empty, scheduled, running,
complete, overdue, failed, loading, stale, error and revoked permission. Reuse
shared tokens, labelled native controls, keyboard focus and 44px targets. State
refresh uses the existing ten-second parent refresh; writes retain entered values
and a captured revision. Failed reads disable writes from unverified state.

## Schema delivery

Use the [verified schema extension](schema-extension.md). Keep one SQL source and
one generated contract. Add only new ordinary tables and their indexes; no numbered
migration files, replay of old DDL, or replacement of the historian volume.
Verify a restored database copy before extending the main installation.

## Design checks

Check 1 traced the current forecast worker boundary, projection persistence,
capability matrix, account-lock helper, schema grants, installer contracts and
parent refresh. It found that forecasts require human attribution and that saved
runs already retain complete inputs. The design therefore keeps an explicit
schedule owner and execution origin, rechecks authority, and saves job completion
with the result. Reusing the locked authority reader avoids a broader user grant.

Check 2 passed 60 combinations of role, state and viewport (1440, 390 and
320 CSS pixels), covering keyboard confirmation, reader restrictions, reflow and
stale/error gates. Phone rendering was inspected. A separate calculation checked
21 timing cases across three intervals, including exact boundaries and 30-day
gaps. Every due case yielded one slot and a next slot after the database clock.

The second logic review found and removed a lock-order cycle: workers must lock
the owner before the schedule/job, matching API account locks. Competing workers
recheck the candidate after that lock. Expired claims cannot commit or replay;
role loss prevents result persistence; edits leave claimed settings intact; a
lost commit response cannot separate a completed job from its result. A backwards
clock waits, and a shutdown leaves an unfinished claim to expire explicitly.

## Verification plan

Use real TimescaleDB tests for duplicate claims, atomic completion, owner
revocation, expired claims, conflicting edits, rollback and original-data
preservation during schema extension. Use an injected clock for interval
boundaries and no-backlog behavior. Browser tests cover role separation, revision
conflicts, uncertain writes, retained drafts, status/result distinction, phone
reflow and automated accessibility. Run a real enabled schedule in the isolated
stack, observe its saved result, then disable it. Review and commit after the
required checks, recording unrun gates.

## References

- [PostgreSQL 16 SELECT locking clauses](https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [PostgreSQL 16 explicit locking](https://www.postgresql.org/docs/16/explicit-locking.html)

## Implementation verification on 2026-10-17

- The initial affected Python run passed 80 checks, including real database claims,
  rollback, revoked ownership and expired jobs. The worker suite then passed 31
  checks with shutdown coverage. Separate database checks passed expiry during
  result insertion and schedule edits after a claim. An edited or disabled schedule
  leaves the claimed job's copied inputs intact.
- All 109 API tests passed through the restricted application database account.
  The schedule tests cover reader/operator denial, bounds, explicit confirmation,
  current authority, competing owners, revision conflicts and audit rollback.
  The test harness now requires an application account, and CI supplies its own
  disposable database credential. Fixture setup retains a separate administrator
  connection. CI also runs the extension and forecast transaction tests.
- Forty shared DTO and page tests passed. Six package type checks and six lint
  tasks passed; Python typing passed for 51 source/test files. Both OpenAPI
  contracts validated, and the prose check reported no errors or warnings.
- The browser workflow passed role separation, uncertain-save GET recovery,
  revision conflicts, retained drafts after failed reads, permission loss,
  separate failed/latest-completed states, 320px reflow and automated WCAG checks.
- An actual enabled schedule in the isolated stack created one owned job and five
  points, retained its execution identity, reopened through the saved-run page and
  left live controls and their revision unchanged. The schedule was disabled after
  the check. Root installations keep schedules unconfigured until an administrator
  explicitly saves one.

The real browser check found a wellhead row lock that exceeded the API role's
read-only asset permissions. The fix uses an ordinary metadata read while keeping
account and schedule locks. The restricted API suite and real worker check then
passed. The expiry test initially tried to alter a lease through the worker role;
that correctly failed. Its corrected fixture waits for the database clock to cross
the deadline and proves the final completion check rolls back all inserted points.

Screenshots show an automatic result from the isolated reference well and a phone
failure-state fixture. Automated accessibility checks do not replace assistive
technology testing. Remote CI has not run for this commit; physical sleep/wake
verification remains deferred at the owner's request.

### Visual checks

The final browser workflow passed again after the phone confirmation labels were
aligned with their checkboxes. Desktop and phone captures were inspected.

- [Automatic forecast on the isolated reference well](../screenshots/scheduled-forecast.png)
- [Phone failure-state fixture with an older completed result](../screenshots/scheduled-forecast-mobile.png)

### Main installation

The tested API, model, dashboard and installer images were deployed locally.
The installer verified the extended schema and provisioned the restricted roles.
The browser smoke check found 12 model wells, an unconfigured schedule and the
expected reader permissions. Before/after fingerprints matched for all 12 durable
control-state rows. Scheduled jobs remain off until an administrator enables them.

Job and run metadata remain available after a schedule stops. Forecast chart points
follow the existing retention policy; an old completed job does not guarantee its
points still exist. The saved-result endpoint reports expired points explicitly.
