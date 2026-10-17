# Live control and parameter screens

Date: 2026-10-17

## Purpose and scope

Let an operator propose and review a change to the running synthetic well, and
let an administrator inspect and publish model parameters. The existing server
lifecycle remains authoritative. This adds human-facing screens, bounded recovery
reads and a parameter catalogue. It does not change the equations or schema.

## User flow

Controls appears for control:issue; Parameters appears for asset:manage. The
shared capability matrix supplies both decisions. ADMIN does not inherit
operating authority. Direct links still require server authorization.

Controls shows the running revision and values, the durable revision and any
queued command. One form changes one target at a time: choke percentage, master
valve, wing valve, pump or shutdown/reset. A reason is required. Saving creates
an immutable draft. A separate review panel shows the well, target, reason,
expected revision, expiry and status. The creator must check a confirmation
before activation. Cancellation remains available while a control proposal is a draft or queued.
The worker can commit first, so a cancellation response must show the recorded
status rather than promise to undo an applied command. Parameter proposals can
be cancelled only while draft. Shutdown/reset occupies its own command, matching the API contract.

A queued response means the worker has work to do. Applied means the database
committed the controls. The running revision and last successful tick show model
adoption separately. An expired or rejected command needs a new proposal. Never
silently replace a draft revision when status polling observes newer controls.

Parameters shows the Python catalogue: description, unit, bounds, base default
and running value. Published overrides and their revision identifiers come from
the database. Administrators select a parameter, enter a value or remove its
database override, give a reason, and save a draft. Review precedes publication.
Removal restores environment/default/per-well resolution, not necessarily the
literal base default. Publication requires a service restart before existing
models adopt it. The screen does not restart infrastructure.

## System and contracts

The browser uses authenticated API routes. The API keeps service credentials
private and checks current account authority inside transactions. Existing
writes retain idempotency keys, revision checks, bounded deadlines and audit.
Use Hono validators with shared request schemas so browser calls stay typed.

Add bounded recent-proposal reads for each well, ordered newest first with
stable identifiers, pagination and an optional own idempotency-key lookup.
These reads work without the model so a lost response remains recoverable.
Show the user's proposals; other creators' drafts cannot be activated here.
Existing single-proposal reads provide status and event history.

Add an authenticated internal parameter-catalogue read. Python derives it from
ModelDefaults and the running ModelParameters. The API validates its response
with a shared schema before sending it to the browser. Do not duplicate the
parameter list or numeric limits in TypeScript. A stopped or invalid model can
still expose definitions and null running values so administrators can repair it.

## Errors, accessibility and refresh

Use visible labels, units, explanatory text, linked field errors and focused
result/error summaries. Reuse the dashboard panels, type, spacing and focus
styles. Stack the editor before review/history on narrow screens. Tables alone
may scroll inside named keyboard-focusable regions.

After an uncertain write, keep its idempotency key, stop submission and offer
Check outcome. Read by that key or proposal ID; never replay the POST. A reload
can recover from Your proposals. Authentication or permission loss clears
restricted reads. Read failures retain marked data but disable activation from
unverified state. Revision conflict asks the user to refresh and review again.

Parent status refresh remains ten seconds. Selected queued-command status can
refresh on the same cadence. Keep the selected status and permission check on this cadence, including terminal
results, so revoked access clears the view. Do not add a second route-wide polling timer.
A pending request disables duplicate buttons. Hidden tabs pause automatic reads.

## Alternatives

A direct slider would hide the review and audit boundary. Hand-entered parameter
keys would avoid a catalogue endpoint but force users to guess units and bounds.
The chosen forms use the existing lifecycle and authoritative parameter metadata.
A general administration console is beyond this screen's purpose.

## Design checks

Check 1 traced the proposal, activation, cancellation, parameter and current-user
routes through shared schemas, control-service transactions and Python resolution.
It found two gaps: missing proposal recovery lists and missing parameter metadata.
It also confirmed separate administrator/operator permissions, creator ownership,
24-hour draft expiry and restart-only parameter publication. The design above
addresses those gaps without new SQL or a duplicate parameter source.

Check 2 passed 57 applicable screen/state combinations at 1440, 390 and 320
CSS pixels before application code changes. It checked headings, reflow,
keyboard help, disabled stale inputs and restricted forms. Phone screenshots
were inspected. Review replaced the cramped proposal table with stacked cards
and corrected the illustrative parameter bounds against the Python defaults.
The first harness attempt referenced a non-existent package path; the next
checked the fieldset instead of its input. The corrected browser checks passed.

## Verification

Exercise the real API middleware with reader, operator and administrator users.
Check bounded history, another user's proposals, revocation, invalid keys and
catalogue errors. Test Python catalogue authentication, actual metadata and null
running values. Browser tests cover keyboard/forms, draft review, no replay,
conflicts, permissions, mobile layouts and explicit restart wording. Use an
isolated stack for actual command and parameter publication. Review and commit
only after these checks pass; record unrun gates explicitly.

## Verification on 2026-10-17

Both final browser workflows passed in 3.3 minutes. They covered the existing
workspace accessibility flow and the new operating screens: linked validation,
review focus, stale revision protection, uncertain draft/activation responses,
GET outcome recovery without another write, queued cancellation, permission
loss, parameter publication and 320px reflow. Axe found no violations for the
selected WCAG A/AA rules in the tested states. These checks used Chromium and
do not establish full accessibility conformance.

A separate browser used actual OPERATIONS and ADMIN accounts in the isolated
installation. A reviewed command advanced the revision exactly once, reached
the running model and appeared in the historian. Reload preserved its record.
The check restored the original control target. Parameter publication left the
running value unchanged; restart adopted it. Removing the override and restarting
restored the original value. The main installation and its controls were untouched
by that test.

The 15 model HTTP tests passed, including authenticated catalogue reads and
definitions for a model without running parameters. Python lint, formatting and
type checks passed. The first API pass passed 14 tests. A further concurrency
review added a regression for proposal recovery during a slow parameter read and
for authority changes while that read waits. All 15 API tests then passed.
All six workspace type tasks and all six lint tasks passed, with two cached
tasks in each run. Both OpenAPI documents and the prose/diff checks passed.

Review corrected a shared-layout focus conflict, scroll spacing under the fixed
header, persistent errors after successful outcome discovery, stale proposal
actions and parameter-read locking. The first operating browser attempt failed
its proposal-focus assertion; the corrected final workflow passed. Queued
cancellation required a design correction after checking the existing server
lifecycle. Its revised desktop and phone mockups passed before the screen change.
No schema change or numbered migration was needed.

Final image checks captured the desktop control and parameter screens and the
phone control screen. The focused proposal heading cleared the fixed header;
the confirmation label measured at least 44px high. Saving and cancelling the
capture draft preserved the control revision. The tested images then replaced
the local API, model and dashboard containers. All six services reported healthy.
Stored control records matched exactly before and after the restart. A browser
confirmed login, 12 current wells, 216 readings, preserved running revisions and
choke targets, and the reader restriction on the control page.

Deployment helper attempts first failed because `python` was unavailable and
then because a nested SQL quote was invalid. Neither attempt changed containers.
Passing SQL on standard input with Python 3 succeeded. The browser helper first
used the system Node 18, which Playwright rejected; the Node 24 run passed.
