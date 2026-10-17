# Dashboard usability and delivery

Date: 2026-10-17

## Purpose

Help a new user inspect a well, understand its readings, and compare a proposed
change without confusing a saved calculation with the running model.
The owner requested design, two design checks, implementation, tests, review,
and a commit for each piece of work.

## Delivery order

1. Improve the existing dashboard and finish the local Milestone 10 checks.
2. Add live control and parameter screens using the existing command lifecycle.
3. Add historical scenario starting points.
4. Add scheduled forecasts.
5. Add the optional metrics stack.
6. Design and implement alarm acknowledgement and shelving.

Each later piece needs its own design and two recorded checks before code.
Contributor consent and remote publication remain separate delivery actions.
Field calibration needs measured data. The owner deferred physical sleep/wake.
Continue using one SQL schema file, without numbered migrations.

## Current problems and changes

| Area | Current problem | Proposed change |
| --- | --- | --- |
| Navigation | Help and Settings are unavailable entries | Offer a working Guide; remove the empty Settings entry |
| Overview | No explanation of how the views fit together | Offer a short expandable starting guide and a clear well list |
| Monitoring | Diagnostics come before readings | Put readings and alarms first; expand model diagnostics when needed |
| Forecast | “Horizon” and sampling lack context | Explain duration and the interval between saved chart points |
| Scenarios | Draft controls sit beside an unrelated saved result | Label the new draft and saved result separately; explain that edits need a new run |
| Results | Baseline and confidence assume prior knowledge | Explain the comparison and score; keep limits next to the result |
| History | Identifiers compete with names and actions | Emphasise names, dates and reopening a result; keep details available |
| Alarms | Dense opening paragraph; inconsistent layout | Show the task first, explain persistence in optional help, label trigger values |
| Analytics | Database terminology and a colour-only severity ring | Explain fleet averages and show severity counts in text |
| Login | Placeholder-only fields and an unresolved photo source | Use visible labels, inline errors, password-manager support and an original text layout |

## Shared layout and wording

Keep the existing colour and spacing tokens, focus outline, chart patterns and
44px controls. Apply the same page heading, panel and table styles everywhere.
Use Overview, Alarms, Analytics and Guide in the main navigation. Within a well,
keep Monitoring, Forecast, Scenarios and Saved runs in that order.

Each page starts with its purpose and next action. Optional help uses native
expandable sections. Users can open it with a keyboard, and no overlay covers
the current task. The Guide contains a first-run walkthrough and short terms.
The scenario walkthrough describes exact inputs but never submits automatically.
No global simulation notice or automatic tour is added.

Monitoring shows recent readings, active alarms, installed controls, then trends.
Model version, tick times and recovery details sit in an expandable section.
Data age and model availability remain visible in the well header.

Keep numerical validation in the shared request schemas. Labels explain units
and helper text explains timing. A scenario starts at minute zero of the copied
state. The baseline uses the same starting state with unchanged controls.
Changing a draft never edits an existing result or applies a live command.
Every explicit run creates a saved result. Show its original inputs separately.

## States and responsive behaviour

The [usability gallery](milestone-9/mockups/usability.html) covers the workspace, fleet and account screens.
Ready, empty, loading, stale, error and permission states apply to data screens.
Login adds invalid inputs; Guide contains static content and needs no data state.
Keep reading order on mobile. Stack forms before results below 760px.
Only labelled, keyboard-focusable tables may scroll horizontally.

Retain successful data after failed reads, mark it unverified, and offer retry.
Show the last successful check time for Alarms and Analytics. A check timestamp
does not establish that the source readings are fresh; charts keep their own
sample times. Authentication and permission failures clear restricted content.
Never replay a write after a timeout. Link to history when its outcome is unknown.

Login has visible field labels, autocomplete hints and a password visibility
button. Allow paste and password managers. Show validation and server errors in
the form, focus an error summary, and retain the username after a failed attempt.

## Design checks

### Check 1: task and contract review

Source reviewed: dashboard routes, projection form/result, request schemas,
model status, control responses, alarm and analytics responses, and ADR 0032.

The design preserves automatic read refresh, explicit writes, immutable saved
results and server permissions. It does not describe fleet averages as an
individual well. Alarm trigger values need an explicit label because they do
not represent the latest reading. A new Guide link requires a real route.
Corrections: retain read-check timestamps on failure; explain trigger values;
keep original run inputs separate from the current form draft.

### Check 2: rendered design review

Completed before application edits. Chromium passed 147 applicable screen,
state and width combinations at 1440, 390 and 320 CSS pixels. Checks covered
page reflow, one main heading and keyboard disclosure controls. Desktop and
phone screenshots were inspected. Corrections added compact navigation,
explicit pressure units, stacked control labels and a valid login error link.
These are design checks; the implemented application still needs its tests.

## Verification plan

Run types, lint and focused unit checks. Build the dashboard and exercise actual
forecast/scenario saves, history, draft retention, permissions and read failures.
Extend browser checks to Guide, Login, Alarms and Analytics, including keyboard
use, automated accessibility checks and narrow layouts. Refresh screenshots
and the demo instructions after the implementation passes.

Use an isolated clean export and a new Compose volume for the final startup
check. Preserve the running stack and its data. Review the complete diff and
commit on the existing branch after the checks pass.

## References

The W3C guidance on [consistent navigation](https://www.w3.org/WAI/WCAG22/Understanding/consistent-navigation.html),
[labels and instructions](https://www.w3.org/WAI/WCAG22/Understanding/labels-or-instructions.html),
and [accessible authentication](https://www.w3.org/WAI/WCAG22/Understanding/accessible-authentication-minimum.html)
informs these checks. Automated checks do not establish full WCAG conformance.

## Account and missing-page review, 2026-10-17

The final route inventory found a public registration form behind an API that
requires user:manage. Keep account creation available to administrators.
Other visitors get an explanation and a sign-in link. Administrators reach the
form through Create account in the header. Compact and desktop navigation
share one menu definition, including Guide. Add visible labels,
field errors, pending state and a success message. New accounts retain USER
as their default role. A failed request must not repeat automatically.
An uncertain creation response directs the administrator to check existing
accounts before another attempt.

The missing-page view needs a main landmark, a descriptive heading, keyboard
focus and a dashboard recovery link. These changes reuse the shared layout.

Design check 1 read the registration API, current user response, capability
matrix and both routes. The API requires user:manage, keeps the administrator
session and creates a USER unless a role is supplied. The revised UI preserves
those contracts. The gallery now includes account and missing-page views.
Design check 2 passed 18 rendered combinations at 1440, 390 and 320 CSS
pixels, before application edits. Each had one main heading and no page
overflow. The phone account and missing-page screenshots were inspected.

## Delivery evidence, 2026-10-17

The clean candidate export contained 340 source and documentation files.
It used only .env.example, separate loopback ports, a separate network and
new storage. All six service images built. Schema setup exited 0, and all
six long-running services became healthy. The running local stack kept its data.

A real browser signed in on the isolated installation and read 12 current
wells and 216 latest readings. It saved a scenario, reloaded that result,
opened Guide and confirmed unchanged installed controls and revision.

The shared-contract and frontend suite passed 37 tests. The API passed 95
integration tests against a disposable TimescaleDB database. The model HTTP
suite passed 14 tests after granting its temporary local servers socket access.
The first full browser pass passed four workflows before the final account
and missing-page additions. The final five-workflow pass passed in 5.9 minutes.
It covered account access, missing-page recovery, Guide, fleet views, saved
analysis, permissions, read failures, keyboard use and responsive layouts.
The account form test used mocked administrator responses; the API suite
verified server authorization against the disposable database.

The final wording-only build passed the 37 unit tests and the complete saved
analysis browser workflow in 3.1 minutes. Its first browser attempt found an
assertion that still expected the old 404 wording. The 404 and request-limit
assertions now match the revised messages; the rerun passed. The eight screenshots
come from that final build.

Six workspace type tasks and six lint tasks passed. The later account and
header changes passed the affected frontend checks. Prose checks reported
no errors or warnings. Review corrected retained-history pagination after a
failed read, restricted history after a 403 response, compact navigation,
login focus timing and collapsed diagnostic panels.

The repository description and topics match the implemented features. Licence
PR 6 remains open, with no comments or reviews when checked on this date.
Contributor consent, remote publication and remote CI remain delivery items.
The owner still defers physical host sleep/wake. These checks do not establish
field calibration or external accessibility certification.
