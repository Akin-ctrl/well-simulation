# ADR 0031: Optimize The Demo For Fast Reviewer Understanding

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Partial

## Context

This repository is a portfolio project. Reviewers should quickly understand what the system does, why it is relevant to industrial software, and how to run or inspect it.

The project should communicate industrial software range: process simulation, industrial protocol ingestion, time-series storage, alarm logic, APIs, dashboards, and disciplined architecture.

## Options Considered

### Option A: Let reviewers infer value from the folder structure

- Pros: No extra work.
- Cons: Reviewers will miss the project value.

### Option B: Provide deep technical docs only

- Pros: Strong for detailed review.
- Cons: Too slow for first impression.

### Option C: Provide a clear 2-5 minute reviewer journey

- Pros: Makes the value obvious quickly while preserving depth through docs and ADRs.
- Cons: Requires maintaining README, demo script, and screenshots.

## Decision

Use **Option C: optimize the project for a clear 2-5 minute reviewer journey**.

## Reviewer Journey

The intended reviewer flow is:

1. Read the README and understand the project in under 2 minutes.
2. Run the local stack with Docker Compose.
3. Open the dashboard.
4. See live simulated wellhead telemetry and alarms.
5. Run or inspect a what-if scenario.
6. Inspect API and architecture documentation if interested.

## Primary Demo Story

The primary demo should show:

- live wellhead telemetry
- Modbus-style industrial ingestion
- TimescaleDB historian storage
- alarm generation
- model/twin status
- a scenario such as closing a choke and observing lower flow and higher upstream pressure

## Consequences

- README quality is part of the product.
- Docker Compose must be reliable.
- Dashboard should prioritize meaningful live data over visual polish.
- Scenario capability should be implemented early because it demonstrates digital twin behavior.
- Screenshots, diagrams, and demo scripts should be kept current.

## Amendment 2026-10-16: Keep monitoring and analysis in one wellhead workspace

### Context

Milestones 4 through 8B now provide the running model, historian, saved
projections, and live control APIs. The browser previously showed only monitoring.
A reviewer needed to construct API requests to see a scenario.

### Options considered

- Keep API examples as the main demo. This gives precise requests but asks a
  reviewer to assemble the comparison themselves.
- Add independent analysis pages. Each page would be simpler, but moving between
  monitoring and a scenario could lose the selected well's context.
- Add Monitoring, Forecast, Scenarios, and Saved runs within each wellhead
  workspace. This keeps the selected well visible and reuses the existing model.
  This option is chosen.

### Decision

Use a compact fleet table and a shared wellhead header. Keep the existing light
theme, blue actions, white panels, and purple analysis line. Distinguish baseline
and scenario with labels and line patterns as well as colour. Stack the form and
chart on small screens and provide the plotted values in a table.

Use existing React Router loaders and form actions, shared request validation,
and saved projection tables. The public API adds status, bounded history, and
the original inputs and signal units on saved reads. Do not introduce a second
data cache or another schema file for these screens.

Scenario drafts survive status refreshes. Creation uses one explicit POST with
no automatic retry. A lost response directs the user to saved history because
the server may already have completed the run. History is shared with authorized
dashboard readers; model-copy isolation does not imply owner-only visibility.

The installed-control panel is read-only. Live command forms remain separate
future work, following ADR 0007 and the authority rules in ADR 0038.

### Consequences

**Good.** A reviewer can monitor, compare, and reopen results in the browser.
Saved inputs and explicit model times make each chart easier to interpret.

**Costs.** Browser verification must cover mobile layouts, session failures,
stale data, invalid forms, expired results, and uncertain submission outcomes.
Screenshots and the demo script must follow the working application.

**Accepted risks.** History can contain results whose points have expired.
Confidence remains heuristic, and threshold summaries can miss events between
returned samples. The UI explains these limits beside the results.

### Related

- [Milestone 9 design](../design/twin-core-milestone-9.md) records screens, time semantics, and verification.
- [ADR 0007](0007-control-and-what-if-boundaries.md) defines the separation from live control.
- [ADR 0025](0025-forecast-horizon-confidence.md) defines the confidence limitations.


## Amendment: explain the dashboard workflow (2026-10-17)

### Context

The owner found the dashboard confusing after the initial browser checks passed.
The pages exposed valid data but assumed knowledge of drafts, baseline controls,
model clocks and saved calculations. Technical checks had not established usability.

### Decision

Lead each page with its task. Add an optional Guide and contextual instructions.
Keep the running well, a new draft and a saved result distinct. Put diagnostics
in an expandable section, retain visible data age, and explain result limitations
beside the chart. Use the same panels, headings and recovery patterns across pages.
Replace the login photograph with an original text layout and labelled fields.

The owner requested two design checks before implementation. The
[usability design](../design/dashboard-usability.md) records both checks and the
rendered gallery. Later control and analysis features follow the same sequence.

### Consequences

Users can learn the available workflow without an automatic tour. Help needs to
change when a feature changes. Login, Guide, Alarms and Analytics join the
browser checks. Automated accessibility results still need human usability review.
