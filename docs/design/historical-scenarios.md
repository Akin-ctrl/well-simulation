# Historical scenario branches

Date: 2026-10-17
Status: Implemented and verified locally
Related decision: [ADR 0027](../adr/0027-historical-branching.md)

## Purpose and user flow

A user can compare a proposed control schedule with unchanged controls from an
exact saved well state. The Scenarios page starts with Current state selected.
Choosing Saved snapshot opens a time finder. The user chooses a time in their
browser's displayed time zone, searches, and explicitly selects one candidate.
The form shows the candidate's exact time and compatibility before Run scenario.
Forecasts continue to use current state. A scenario changes only private model
copies. It cannot change live controls or the saved source snapshot.

The result and Saved runs identify Current state or Saved snapshot. Result
metadata includes the snapshot identifier and base time. The existing name,
duration, sample interval, schedule, baseline, charts and table keep their roles.

## Data and compatibility

The existing `twin_state_snapshot` table stores complete process state every
30 seconds, with model version, parameter fingerprint, tick count and skipped
ticks. Its primary key contains both `snapshot_id` and `simulated_at`.
Snapshot identifiers travel as decimal strings, bounded by signed PostgreSQL
bigint, because JavaScript numbers cannot represent every bigint exactly.
Requests include both key fields and the well identifier.

Snapshots do not store parameter values. Branching therefore requires the
installed model version and the current resolved parameter fingerprint to match
the snapshot. Decode every state field, verify its UTC timestamp against the row,
validate controls and physical bounds through the model boundary, and reject
future, missing, malformed or incompatible snapshots. Never replace a failed
selection with current state. A live model tick need not be current for a valid
historical branch, but resolved parameters must exist for the active well.

The snapshot finder returns at most 20 candidates, newest first, at or before the
requested time. Search covers the preceding 24 hours and no more than 89 days
before request time. Users can change the search time to inspect older periods.
Use 21 rows to report that older candidates exist; no unbounded offset scan.
Each candidate carries its exact composite key, version and compatibility reason.
Show the first three cards initially, with an explicit Show more snapshots button
for the rest, so the finder does not bury the scenario inputs.
The server repeats compatibility and state validation when running the scenario.
A selection can become invalid after a parameter change or retention job.

## Service boundaries

The public API requires `scenario:run` for finding snapshots and executing a
historical scenario. Existing read limits, analysis write limits and body limits apply.
The model service owns snapshot reads because it already holds the read grant
and understands state compatibility. Its internal snapshot route requires the
service token before checking well existence. Query fields are strict and bounded.
Snapshot reads use the existing read semaphore and three-second database timeout.
Projection work shares the existing two-slot semaphore and bounded 5–120 minute
window, with at most 16 control changes. Browser requests never carry the token.

A scenario request adds optional `baseSnapshot: { snapshotId, simulatedAt }`.
Omission retains current-state behavior. A finder GET accepts `at` in UTC and
returns `wellheadId`, `checkedAt`, `items` and `hasMore`. Each item contains
`snapshotId`, `simulatedAt`, `modelVersion`, `compatible` and a bounded reason.
The selection must refer to the route's well. Missing source returns 404;
incompatible or invalid source returns 409. Dependency failures return 503.
Public error text explains the next step without exposing internal exceptions.

## Model logic and time

Create an immutable `ProjectionBase` from the selected process state, its saved
controls and the matching resolved parameters. Both scenario and baseline start
from that same copy. Do not run live recovery or settle the snapshot toward now.

The model advances by `t_n = t_base + n * MODEL_STEP_SECONDS`.
`t_base` is `base.state.simulated_at` in UTC; `n` counts model steps;
`MODEL_STEP_SECONDS` is one second. Saved chart samples occur at
`t_base + k * step_seconds`, where `k` counts samples and `step_seconds` is the
requested sample interval in seconds. Schedule minute offsets are relative to
`t_base`. `generated_at` remains the actual execution time.

Current alarm rules are copied at execution and saved with the run. The UI says
that historical scenarios use those rules, rather than claiming to reconstruct
past alarm configuration. The existing heuristic confidence formula remains
unchanged. Add `historical_snapshot` as an explanatory reason without an invented
age penalty or calibrated probability. Missing recovery metadata stays unknown;
we do not invent a past recovery time.

## Persistence and retention

Save `branchOrigin` inside the existing immutable `scenario_run.base_state` JSON:
`{ kind: 'snapshot', snapshotId, simulatedAt }`, or `{ kind: 'current' }`.
The existing `base_simulated_at` remains queryable. Runs retain complete base state,
parameter values, initial controls, schedule and alarm rules. Source snapshot
expiry therefore does not remove saved run inputs. Legacy rows without origin
mean current state, the only source supported when those rows were written.
Expose origin in fresh responses, saved responses and summary history. Reject
malformed stored origin instead of relabelling it current.

No SQL change or numbered migration is needed. This amends ADR 0027's proposed
separate provenance columns to use the existing immutable base-state record.

Both snapshot and scenario-point retention use simulated timestamps and a 90-day
policy. A newly created historical run does not receive 90 days from creation.
The finder limits sources to 89 days old to avoid immediately expired points;
older branches can still have much less chart retention than current runs.
Show this before execution and with historical results. Expired points retain
the existing 410 response and history entry. Do not promise an exact deletion
time because retention operates on TimescaleDB chunks.

## Screen states and accessibility

Mockups cover current, ready, selected, loading, empty, incompatible, stale,
error, permission and saved-result states at desktop and phone widths. Reuse
shared spacing, typography and colors. Use labelled native date/time and radio
controls, visible keyboard focus, text compatibility reasons, and a status
message for search completion. Candidate cards wrap at 320px. The time field
states the browser time zone; the server receives UTC. No color-only state.

Search and selection keep the user's scenario name and control schedule. Editing
the search time clears the old selection, so the displayed search and submitted
base cannot disagree. A search failure leaves the schedule but clears selectable
candidates. No selection disables historical execution. Revoked permission clears
restricted candidates and disables the form. Historical metadata stays attached
to the saved result even when the user edits a new draft.

## Alternatives

Raw-reading reconstruction needs a state estimator and cannot recover the full
internal process state directly. Replaying an old parameter fingerprint with
new parameters would change the meaning of the historical branch. Storing full
parameter sets in future snapshots could support more versions later, but it
would require a separate compatibility design and schema change.

## Design checks

Check 1 read ADR 0027, the canonical SQL snapshot and scenario tables, recovery
validation, projection runtime/confidence, persistence, shared request/response
schemas, API saved reads, and the scenario form. It found the parameter-value gap,
composite identity and retention based on simulated time. The design above
addresses each before implementation. The confidence code has no general state
age penalty; the design keeps its formula and labels historical origin separately.

Check 2 passed all 30 screen/state combinations at 1440, 390 and 320 CSS
pixels: reflow, headings, keyboard selection and blocked execution. Phone
inspection found cramped card padding; the mockup spacing was corrected and
the same checks reran before implementation.

## Verification plan

Model tests cover exact snapshot lookup, bigint identity, wrong well/time,
compatibility, malformed state, source age, private state isolation, point times,
saved controls, metadata persistence, capacity release and legacy current runs.
API tests cover authentication, strict finder queries, safe upstream errors,
provenance through saved reads/history and the unchanged forecast contract.
Browser tests cover selection, time conversion, retained schedule, permission
loss, no silent fallback, saved reload, mobile reflow and automated accessibility.
An isolated real stack must run and reopen a historical scenario while preserving
live controls and the source snapshot. Record failures and unrun gates before
review and commit.

## Verification on 2026-10-17

The affected Python run passed 45 tests across historical state, projections and
the HTTP boundary. A separate real TimescaleDB test passed with the model's
restricted database role. It selected an exact retained source, rejected a wrong
well and a timestamp one microsecond away, saved complete run metadata and
points, and confirmed the source stayed unchanged. The other database test in
that module was deselected in this targeted run.

The projection API suite passed 13 tests. Shared schema and frontend unit tests
passed 15 tests. All six workspace type tasks and all six lint tasks passed,
with three cached tasks in each run. A final website type check also passed.
Python types passed across 33 source files; affected lint and formatting passed.
Both OpenAPI specifications and prose/diff checks passed.

The real isolated browser workflow passed in 41.6 seconds. It ran and reopened
a historical scenario, checked exact origin metadata and unchanged live controls,
then covered empty and incompatible candidates, late search responses, stale
selection rejection, retained drafts, dependency errors, permission loss, 320px
reflow and the selected Axe WCAG A/AA rules. This Chromium check does not establish
full accessibility conformance.

Initial checks exposed a test fixture using base defaults instead of per-well
parameters, a wrong injected-clock keyword and a missing origin in an older
frontend fixture. These were corrected. The first HTTP test command could not
open sockets in the sandbox; the run with loopback access passed. Lint rejected
an intentionally oversized number literal in a rejection test; explicit Number
conversion preserves that test case without a lossy source literal.

Review added exact microsecond comparisons, strict UTC precision, a compact
three-candidate opening list, stale-selection clearing and a light native-control
scheme matching the workspace. Saved runs retain historical provenance after
source expiry. No SQL change, live-control mutation or numbered migration was
required. Remote CI and additional assistive-technology checks have not run.

Final image verification confirmed the light native-control scheme, three initial
candidate cards, explicit expansion to six, and desktop/phone layouts without
overflow. The screenshots in `docs/screenshots` came from that running image.
The tested images then replaced the main local API, model and dashboard services.
All durable control records matched before and after restart. A final browser
confirmed 12 current wells, selected a compatible source, saved and reopened its
exact branch, and verified unchanged live controls.
