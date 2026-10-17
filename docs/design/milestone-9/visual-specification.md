# Milestone 9 visual specification

Date: 2026-10-16

## Purpose and design artifacts

The [interactive mockup gallery](mockups/index.html) covers Fleet, Monitoring,
Forecast, Scenarios, and Saved runs. Select a screen and state in its review
header. That header and its illustrative fixtures belong to the design artifact
only. The application continues to use API responses.

Open the gallery through a local static server at the repository root, or open
its HTML file directly. Query parameters such as `?screen=monitoring&state=stale`
make each design reproducible. Desktop and mobile share the same document and
responsive rules. The gallery is the proposed layout; application screenshots
in `docs/screenshots` record the implemented result.

## Shared visual rules

Use the values in the [shared token stylesheet](../../../monorepo/apps/website/app/styles/tokens.css). Components reference these
variables so later changes do not require independent colour edits.

| Element | Specification |
| --- | --- |
| Surfaces | Page #f4f6f9; panels #ffffff; separators #dce1e8; control boundaries #788598 |
| Text | Primary #1d2637; secondary #586477; system sans-serif; body 16px and 1.5 line height |
| Hierarchy | Page heading 30px/1.25, weight 650; panel heading 18px/1.4, weight 650; subheading 16px; labels 14px; captions at least 12px |
| Actions | Blue #023bb5 with white text; visible focus outline #344dc4, 3px wide with 3px offset |
| Status | Green #17613c on #eff8f2; amber #693f00 on #fff7e7; errors #a31b32 on #fff1f3; always include words |
| Chart series | Baseline #59677d, dashed; scenario #553afe, solid; historian palette uses dark blue, green, purple and brown with distinct dash patterns |
| Spacing | 4, 8, 12, 16, 20, 24 and 32px; 20px panel padding and grid gap; 16px on phones |
| Shape | Panels 9px radius; controls 6px; status badges 5px; thin borders and no decorative shadows |
| Form fields and workspace action buttons | At least 44px high; visible label; consistent focus and disabled treatment; inline text links remain underlined |
| Numbers | Tabular numerals; units in headings or beside values; missing values read “Unavailable”; zero stays zero |

## Layout and responsive behaviour

The desktop shell has a collapsible navigation column and a content area that
can shrink. Hide the desktop column below 1280px and show the header navigation.
Analysis uses a 300px input column and a flexible results column, reducing the
input column to 260px below 1000px. Below 760px, forms and results stack in reading
order. Small metric groups also stack when their text would become cramped.

At 320 CSS pixels, navigation wraps, labels and summaries wrap, and controls use
the available width. Only inherently two-dimensional tables scroll horizontally,
inside labelled keyboard-focusable regions. Charts remain within their panel and
have a data table alternative. Long names and identifiers wrap. Use a static
header on narrow screens so it cannot cover most of a zoomed viewport.

A skip link moves focus to the main content. Navigation to another page moves
focus to the main heading region and updates the document title; background
polls do neither. Sticky desktop headers have enough scroll offset to keep
focused elements visible. Reduced-motion preferences disable shell transitions
and chart animation.

## State matrix

Every screen has a ready state in the gallery. The following states are part of
the design and implementation contract, including first load and later refresh.

| Screen | Loading | Empty | Stale | Error | Permission |
| --- | --- | --- | --- | --- | --- |
| Fleet | Named loading status; retain old rows during a poll | Distinguish no configured wells from no filter matches | Show age and stale words per row; overdue model check independently | Initial read gets a recovery page; optional model failure keeps historian rows | Denied initial read gives an access explanation and sign-in/overview recovery |
| Monitoring | Load shell; retain readings while refreshing | Missing readings and missing history have separate explanations; installed controls can still load | Server age plus browser elapsed time; original timestamp remains visible | Optional model/control failure stays within its panel; required read failure has a retry page | Required read denied stops display; no fabricated zero readings |
| Forecast | Announce calculation; preserve draft and previous immutable result | No selected result offers run and saved-history paths | Mark current model status overdue; never relabel an immutable result as live | Keep draft; distinguish validation, expired session, forbidden, expired result, busy service and unknown submission outcome | Disabled form with explanation if capability is absent; server denial keeps draft |
| Scenarios | Same as Forecast; no automatic repeat submission | Same as Forecast | Same as Forecast | Field errors link to controls; timeout links to scenario history | Same as Forecast |
| Saved runs | Announce history read and retain existing rows | Distinguish no runs, no search matches and an empty page | Show when the list was checked; saved results keep their creation timestamps | Recovery and manual retry; expired selected result explains retention | Access explanation; no claim that model-copy isolation means owner-only visibility |

The gallery includes invalid-input variants for Forecast and Scenarios. Both
use the same error-summary and field-message design.
Fleet, Monitoring and Saved runs do not expose model-input validation.
Initial required-read failures show a recovery view with an explicit GET retry.
Later failures retain readings and drafts, mark them unverified, and allow retry. Optional
model failures must not replace historian data. Retaining a draft across a
successful status poll is required; a lost login session requires authentication.

## Charts and forms

Use the returned sample timestamp, display its timezone, and state the unit for
each plotted signal. Do not connect missing samples or treat them as zero.
Historian gaps keep the existing gap rule. Projection samples use their own
returned timestamps. Boolean signals use step lines and text state labels.
Baseline and scenario differ by line pattern as well as colour. Labels and a
plain-language description identify what is plotted. The expandable data table
contains the same plotted samples with bounded pagination and column headings.

Schedule markers identify input times, not exact threshold crossings. Confidence
is a heuristic label with reasons, not a probability or uncertainty band. Show
the earliest returned threshold exceedance and preserve its sampled meaning.

The shared request schemas own all numerical bounds and schedule validation.
Submit validation focuses an error summary with links to each invalid control.
Each invalid field has `aria-invalid` and its own explanatory text. Adding a
schedule step focuses its time field; removing a step focuses a remaining step
or the add button. Limit the editor to the schema's maximum step count.

A pending submit is guarded against duplicates while focus remains usable.
Announce calculation start and completion once. A timeout can have an unknown
outcome, so link to the matching history and require an explicit new submission.
A rejected validation/permission/rate-limit request does not imply a saved run.

## Accessibility verification

Target WCAG 2.2 AA. Check automated rules, accessible names, keyboard-only use,
focus after navigation and validation, chart tables, contrast, reduced motion,
320px reflow, 200% text sizing, and status announcements. Avoid announcing every
one-second age update. A single polite region announces freshness category or
refresh-state changes. Validate actual screen-reader behaviour separately from
automated accessibility-tree inspection and state the tested combination.

The W3C [reflow guidance](https://www.w3.org/WAI/WCAG22/Understanding/reflow.html)
allows two-dimensional tables to scroll within their own region. Its
[status-message guidance](https://www.w3.org/WAI/WCAG22/Understanding/status-messages.html)
requires updates to be available to assistive technology without moving focus.
[Focus must remain visible](https://www.w3.org/WAI/WCAG22/Understanding/focus-not-obscured-minimum.html).
[Playwright's accessibility guidance](https://playwright.dev/docs/accessibility-testing)
explains why automated scans need manual checks. These checks establish evidence
for the tested workflows, not external certification or universal device coverage.

## Design review record

The gallery passed 96 combinations: six states on each of five screens, plus
invalid inputs on Forecast and Scenarios, each at 1440, 390 and 320 pixels.
There were no uncaught browser errors or page-level horizontal overflow. Desktop
and phone screenshots were visually inspected. Phone chart labels were enlarged
following that inspection. The repeatable check is
`monorepo/apps/website/e2e/design.spec.ts`.

The illustrative gallery omits the full signal catalogue and uses representative
rows. The implemented dashboard includes all configured wells and the available
signal groups, using API data. Original saved inputs and confidence explanations
can extend below the chart; they remain in normal reading order.

## Usability amendment, 2026-10-17

The [usability design](../dashboard-usability.md) records a later review of the
complete dashboard. Its gallery adds Alarms, Analytics, Guide and Login.
Use the working Guide in place of the unavailable Help entry. The revised
sidebar is 240px wide. Analysis uses a 320px input column, 280px below 1000px,
and stacks below 760px. Form fields follow one column so their explanations
fit. Model diagnostics expand on request. Drafts and saved results keep
separate labels and inputs. This amendment supersedes the older layout sizes.
