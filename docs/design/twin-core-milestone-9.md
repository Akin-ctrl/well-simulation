# Milestone 9: A Wellhead Workspace For Monitoring And Analysis

Date: 2026-10-16
Status: Design completed, implemented, and verified locally; uncommitted

## Purpose

A reviewer should be able to find a well, see whether its data is current, run a
forecast, and compare a proposed control change with the unchanged baseline.
Milestones 4 through 8B supply the model, historian, saved projections, and control
contracts. This milestone makes those capabilities understandable in the browser.

## Design completion gate

The owner reaffirmed this requirement on 2026-10-16: complete the relevant
visual design before implementing a new screen or a substantial UI change.
Apply the same sequence to later dashboard work.

Before implementation:

- Complete desktop and mobile mockups for every affected view. The original
  Milestone 9 design set includes Fleet, Monitoring, Forecast, Scenarios, and
  Saved runs.
- Show loading, empty, stale, error, and permission states for each view where
  they apply. Record why a state does not apply instead of silently omitting it.
- Define shared spacing, typography, colours, and component states with specific
  values. Record chart rules for units, times, missing values, baseline lines,
  scenario lines, and threshold information.
- Define responsive behaviour: when panels stack, how navigation changes, what
  scrolls, and how labels and chart axes remain readable on small screens.
- Render and inspect the mockups on desktop and mobile. Check browser errors
  and horizontal overflow, and record the results and remaining design gaps.
- Plan keyboard operation, focus order, accessible names, contrast, status
  announcements, and chart alternatives before writing the UI code.

During implementation, validate accessibility in the working application,
including keyboard and screen-reader use, contrast, zoom and reflow, form
errors, and dynamic status announcements. Record each check and any limitations.
A successful render and a keyboard-focus check do not establish full
accessibility coverage.

### Completion of the design set

The first implementation followed only the scenario concept. The owner then
required the missing design work to be completed before further UI changes.
That gap is now addressed by the [five-screen mockup gallery](milestone-9/mockups/index.html)
and [visual specification](milestone-9/visual-specification.md). The gallery covers
ready, loading, empty, stale, error, and permission states, plus validation in
both analysis forms. Its 96 screen/state/viewport combinations rendered without
page errors or horizontal overflow at 1440, 390, and 320 pixels.

The complete mockup set and specification were written and rendered before the
subsequent application changes. Visual inspection found small phone chart
labels, which were enlarged before implementation proceeded. The application
and gallery now import one shared token stylesheet. Working screenshots remain
separate from illustrative mockups.

Implementation adds named collapsed navigation, a skip link, navigation and
result focus, field-specific error descriptions, keyboard focus for schedule
editing, historian chart tables, and reduced-motion behaviour. Failed required
refreshes retain readings and drafts within the mounted view and mark them
unverified. A different asset or an authentication failure cannot reuse that
retained state. Saved history retains its prior list after a failed manual refresh.


## Screen design

Keep the existing light theme, blue actions, and purple analysis lines. Use a
muted page background, white panels, consistent spacing, readable labels, and
aligned numeric columns. Alarm and failure colours accompany words and icons.

The fleet overview uses compact rows instead of repeated large cards. Include
all configured assets, even if they have no readings. Show pressure, production,
freshness, model status, and abnormal-reading counts. Filtering must consider
all available signals so a condition outside the headline measurements remains
visible. Active alarms keep their existing severity order.

Each wellhead has a common identity header and navigation:

| View | Layout and behaviour |
| --- | --- |
| Monitoring | Data and model status first, grouped measurements, active alarms, then existing historian charts. Installed controls remain a read-only view. |
| Forecast | Bounded horizon and sample interval beside a large projection chart. Show model start time, confidence reasons, and sampled threshold exceedances. |
| Scenarios | Schedule editor beside a baseline comparison. Mark scheduled changes on the chart. Reopened results show their saved inputs. |
| Saved runs | Per-well forecast or scenario history, name search within the loaded page, bounded pagination, and direct links to immutable results. |

At narrow widths, panels stack, navigation remains accessible, and wide data
tables scroll inside their own region. Charts keep readable axes rather than
shrinking desktop labels. The same tokens apply to loading, empty, failed, stale,
and permission states. A failed optional model read must not blank historian data.

The local visual study uses illustrative data. Shipped screens use API data.
There is no new global simulation notice. Equipment drawings need a separate
verified layout; the current scope uses readings, control states, and timelines.

## Architecture and alternatives

Use React Router loaders for reads and actions/fetchers for explicit submissions.
Use the existing typed Hono client and shared Zod request schemas. React Router
already owns navigation and refresh, so a second server-data cache would add a
second source of state. Separate pages for every model function would simplify
individual forms but lose well context. The shared workspace preserves it.

The browser calls nginx, then the authenticated API. Only the API contacts
internal twin-core. Internal credentials never enter browser responses.

Use existing prediction_run, scenario_run, and their point tables. No schema
change or numbered SQL file is needed. The public API adds bounded history,
canonical signal metadata, and lifecycle status. Saved responses expose the
stored baseline controls and scenario schedule. Projection creation keeps its
existing bounded model runner and audit records.

Saved runs are readable by users with dashboard:read, following the existing
saved-result policy. Model-copy isolation does not mean owner-only visibility.
Live command entry and parameter publication remain separate future UI work;
this milestone shows installed controls without adding command buttons.

## Time, charts, and calculations

Keep three times distinct: last scheduler tick, last successful process time,
and historian sample time. A scheduler tick can advance while a process is
stopped. Status must show the process timestamp and lifecycle reason. Compute
ages on the server; browser refresh delays must not make an old screen appear
current. A failed refresh labels retained data as unverified.
The browser adds elapsed time to the last server-reported telemetry age. It
marks the model check overdue after two missed refresh intervals. The polling
timer reads the current refresh state and skips a request already in flight.

The existing model integration remains one second. The requested sample interval
only selects output points. Use each point's returned timestamp on the chart.
Do not reuse the historian's fixed two-minute gap algorithm for projections.
Missing values stay missing. Boolean signals use step lines and state labels.
One selected signal and the unit defined in its database catalogue occupy a
chart at a time.

For a scenario, the displayed difference is:

    difference = scenario_value - baseline_value

Both values come from the same projection point and parameter code. The unit is
that signal's canonicalUnit. Do not compare values from different timestamps.

A schedule marker occurs at baseSimulatedAt plus offsetMinutes times 60 seconds.
The model applies it before the following one-second integration step. Chart
markers describe the input schedule, not interpolated threshold crossing times.

Threshold summaries report the first returned sample with warning or critical
risk. They do not promise that no event occurred between samples. Display the
returned heuristic confidence label and reasons. Do not draw statistical bands
or calculate an alarm probability from that score.

## Forms and state

Forecasts accept 300 to 7200 seconds with samples every 5 to 300 seconds. Horizon
must divide evenly by the sample interval. Scenario schedules contain 1 to 16
ordered steps at whole-minute offsets before the horizon. The shared request
schemas own these rules; Python remains authoritative for model execution.

Keep drafts while polling. Do not submit on a timer, navigation, or field edit.
Disable repeat submission while pending. A timeout or lost response can occur
after a run was saved, so show an unknown-outcome message and a saved-runs link.
No automatic POST retry is allowed. History is a discovery aid, not an
idempotency guarantee. Reopening a run never executes it again.

Use explicit loading, no-data, invalid-input, forbidden, expired-session,
not-found, expired-result (410), busy (429/503), and timeout (504) states.
Creation errors keep the draft. A saved result remains visible while another
request is pending, with a clear indication of which inputs produced it.

## Accessibility

Controls have visible labels, errors reference their fields, keyboard focus is
visible, and status messages use appropriate live regions without stealing
focus. Baseline and scenario have text legends and different line patterns.
Provide a table containing the plotted values. Respect reduced-motion settings.
These are implementation requirements, not a claim of external certification.

References: [React Router fetchers](https://reactrouter.com/7.18.4/how-to/fetchers),
[W3C use of colour](https://www.w3.org/WAI/WCAG22/Understanding/use-of-color.html),
[W3C complex charts](https://www.w3.org/WAI/tutorials/images/complex/), and
[W3C status messages](https://www.w3.org/WAI/WCAG22/Understanding/status-messages.html).

## Verification gates

- API tests use the real disposable Timescale database for history, saved inputs,
  pagination, units, permission checks, and expired results.
- Model status tests distinguish running ticks from stale process output and
  handle partial startup and unreachable services.
- Frontend tests cover schedule serialization, missing values, timestamps,
  baseline alignment, and boolean signal display.
- Browser checks cover all views at desktop and mobile sizes, real forecast and
  scenario execution, saved reloads, unchanged live controls, validation,
  retained drafts, dependency failure, session expiry, and result expiry.
- Run types, lint, formatting, relevant tests, OpenAPI validation, and prose checks.
- Record actual results after implementation. Screenshots must come from the
  running UI and must not contain illustrative prototype data.

The browser test paces requests below the existing API limit. It does not
raise that limit or bypass authentication. Real model runs use the seeded demo
account. Fault cases intercept individual browser responses to exercise recovery
without stopping the shared local services.

## Costs and accepted limits

The frontend adds forms and result state that need browser coverage. Polling
uses bounded reads and pauses in hidden tabs. Saved run metadata can outlive its
retained points, so history can contain an expired result. Canonical signal units
remain stable for the supported model versions; changing units later requires
versioned interpretation. Field accuracy still depends on measured calibration
data, as recorded in the model design.

## Verification on 2026-10-16

- 95 API integration tests passed against the disposable Timescale database.
  Coverage includes saved inputs, canonical units, bounded history, pagination,
  authorization, expired points, partial model startup, unavailable model status,
  and configured wells without readings.
- 34 shared-contract and frontend unit tests passed. The six new presentation
  checks cover schedule conversion, validation, missing values, zero values,
  boolean states, and projection samples at 5, 60, and 300 seconds.
- 14 twin-core HTTP tests passed, including protected status reads and the case
  where scheduler ticks continue while the successful process state is stale.
- The complete Chromium browser workflow passed in about 2.4 minutes. It created
  and reopened real forecasts and scenarios, checked their original inputs, and
  confirmed unchanged installed controls and revision. It checked keyboard focus
  and the fleet, Monitoring, Scenarios, and Saved runs layouts at 390 pixels.
- Browser fault checks passed for unavailable model status, an expired result,
  a result linked under the wrong well, expired sessions, forbidden requests,
  request limits, busy services, and timeouts. Drafts survived errors. Timeout
  links selected the matching forecast or scenario history. Saved reloads did
  not repeat POST requests.
- During a 35-second intercepted status delay, telemetry became stale and the
  model check became overdue. Only one status request remained in flight.
- Workspace types, lint, and formatting passed. Python Ruff checks, formatting,
  and mypy passed. Both OpenAPI documents, prose checks, and git diff checks passed.
- The changed API, dashboard, and twin-core images built and were deployed locally.
  All six long-running Compose services reported healthy. Eight screenshots were
  captured from the working application and linked from the root README.

Verification caught an API dependency import outside its owning package, mobile
grid overflow, and a polling callback that retained an old refresh state. These
were corrected. The delayed-response test's cleanup race was also fixed. An
unpaced browser run reached the real API request limit; pacing the test resolved
that failure without changing the limit. The final browser run passed without
uncaught page errors.

The browser workflow is wired into CI, but remote CI has not run. A fresh-volume
start of this complete revision remains a Milestone 10 delivery gate. Database
conversion and physical restore tests were not repeated because this milestone
changes no schema or engine. The owner's physical host sleep/wake deferral remains.


## Design completion and accessibility verification on 2026-10-16

The remaining design work and the resulting implementation are complete for
this milestone's scope. The [detailed verification record](milestone-9/accessibility-verification.md)
records the browser, assistive technology, checks, and limitations.

- All three browser workflows passed in 4.4 minutes. These cover the 96 mockup
  combinations, accessibility and recovery checks, and the complete model demo.
- Axe reported no violations for the selected WCAG A/AA rules in the tested
  views. Keyboard focus, linked form errors, 320px reflow, doubled root text
  sizing, reduced motion, retained drafts and history, and permission states passed.
- Orca 46.1 generated the expected field, error, result-heading and table-region
  announcements in the keyboard scenario workflow. Speech synthesis initialized;
  audio went to a null device. This was a speech-log check, not a listening test.
- All 37 shared-contract and frontend tests passed. All six workspace type tasks
  and six lint tasks passed, followed by the final affected website checks.
- The production dashboard image built and started successfully. All six local
  Docker services reported healthy. Eight application screenshots were refreshed.
- Prose and diff checks passed. No database schema change was needed.

The initial screen-reader setup needed additional temporary dependencies and a
speech-server readiness check. The final workflow completed. Its optional mouse
review and audio-cue features were unavailable. NVDA, VoiceOver, braille, other
browser engines, audible quality, remote CI and a fresh-volume start were not
verified in this pass. These limits do not change the recorded functional results.

## Usability and local delivery verification on 2026-10-17

The [usability design](dashboard-usability.md) records the page changes and two
design checks before implementation. The dashboard now includes optional help,
a first-run Guide, clearer draft and result labels, consistent recovery states,
accessible account forms and navigation that works on narrow screens.

All five browser workflows passed. The 37 shared-contract/frontend tests,
95 API integration tests, 14 model HTTP tests, workspace types and lint passed.
The clean candidate export built all service images and started all six running
services with fresh storage. Schema setup exited 0. Its browser smoke check saved
and reopened a scenario without changing installed controls. This closes the
local fresh-volume gate recorded above. Remote CI, publication and contributor
consent remain open. Physical sleep/wake remains deferred by the owner.
