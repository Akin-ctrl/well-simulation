# Milestone 9 design and accessibility verification

Date: 2026-10-16 (UTC)

## Scope

The review covers Fleet, Monitoring, Forecast, Scenarios, Saved runs, and their
shared navigation. The target is WCAG 2.2 AA. The evidence below applies to the
recorded workflows and browser combination; it is not an external certification.

The [visual specification](visual-specification.md) records the component rules
and state matrix. The [mockup gallery](mockups/index.html) contains illustrative
fixtures. The application screenshots in `docs/screenshots` use real API data.

## Checks and results

| Check | Result |
| --- | --- |
| Mockup coverage | 96 screen/state/viewport combinations passed at 1440, 390 and 320 pixels, without page errors or page-level horizontal overflow |
| Complete browser suite | All three workflows passed in 4.4 minutes, including real analysis, saved reloads, unchanged installed controls and failure recovery |
| Automated accessibility | Axe rules tagged WCAG 2 A/AA, 2.1 AA and 2.2 AA reported no violations in the tested views and error states |
| Keyboard | Named collapsed navigation, skip link, route focus, saved-result focus, schedule add/remove focus, linked error summary and chart tables passed |
| Reflow and text sizing | All five views passed at 320 CSS pixels and with the root text size doubled at a 1280-pixel viewport |
| Reduced motion | Browser checks used the reduced-motion preference; shared-shell transitions and chart animation are disabled |
| Validation | Out-of-range target produced an invalid-field state, a focused summary and a link to the input; no model request was sent |
| Refresh recovery | Required-read failure retained the draft and prior readings with an unverified warning; retry recovered without discarding the draft |
| Saved history | Failed manual refresh retained the prior list; recovery and an empty search passed |
| Permission | Denied analysis retained the editor; denied required reads produced an accessible recovery page |
| Screen reader | Orca 46.1 with headed Chromium and eSpeak NG generated the expected announcements for the scenario workflow |
| Shared contracts and presentation | 37 tests passed, including schema errors mapped to stable schedule-row identifiers |
| Static checks | Six workspace type-check tasks and six lint tasks passed; the final affected website checks also passed |

The browser harness uses Playwright 1.63.0 and its bundled Chromium. It paces
requests below the existing API limit and uses the seeded demo account. Individual
response failures are intercepted in the browser. The test does not stop shared
services, raise request limits, or modify installed well controls.

## Contrast

Ratios were calculated from the actual shared CSS token values using relative
sRGB luminance. The text pairs exceed 4.5:1. The focus and input-boundary pairs
exceed the 3:1 non-text threshold.

| Pair | Contrast ratio |
| --- | --- |
| Main text / white panel | 15.17:1 |
| Secondary text / page background | 5.54:1 |
| White action text / blue button | 9.15:1 |
| Success text / success background | 6.89:1 |
| Warning text / warning background | 8.53:1 |
| Error text / error background | 6.93:1 |
| Baseline line / white | 5.74:1 |
| Scenario line / white | 6.14:1 |
| Focus indicator / white | 6.99:1 |
| Input boundary / white | 3.75:1 |

Chart series also use different line patterns, visible legends, units, and a
labelled data table. Colour is not the only way to identify a series or status.

## Orca workflow and limits

Orca ran in an isolated Xvfb desktop and D-Bus session. Its packages were extracted
to `/tmp`, without changing installed system packages. Speech Dispatcher initialized
successfully with eSpeak NG and an ALSA null output device. The check inspected
Orca's generated speech log and browser focus assertions; no person listened to
the synthesized audio.

The workflow opened a well, navigated to Scenarios, entered an invalid target,
submitted with the keyboard, followed the error link back to the input, corrected
the schedule, ran it, and opened the plotted-values table. Selected output:

```text
Check your inputs clickable.
Step 1 target: Enter a target from 0 to 100%. link.
Target (%) 101 spin button required.
invalid entry.
Enter a target from 0 to 100%.
Choke adjustment heading level 2 clickable.
View plotted values as a table toggle button expanded.
Projection values region clickable.
```

Orca also announced the telemetry category and loading-view status. The visible
reading age changes each second, while the live region changes only when the
freshness category changes. This avoids announcing each second of the age counter.

The first setup attempt lacked the speech backend and its dependencies. The final
run waited for Speech Dispatcher readiness and confirmed successful initialization.
Optional mouse-review and non-speech audio-cue support were unavailable because
Wnck and GStreamer were absent. Keyboard navigation, speech generation and form
feedback were exercised. Mouse exploration, audible quality, braille, NVDA,
VoiceOver, Firefox and Safari were not tested.

The 320px check exercises the layout width described in the W3C reflow guidance.
The text-sizing check changes the root font size; it is not a claim that every
browser's native zoom implementation was tested.

## Repeating the automated checks

From `monorepo`, with the local Docker application running:

```sh
pnpm check-types
pnpm lint
pnpm exec vitest run apps/website/app packages/dto
pnpm --filter @well-simulation/website test:browser
```

`e2e/design.spec.ts` validates the gallery without API requests.
`e2e/accessibility.spec.ts` checks semantics, focus, accessibility rules, reflow,
validation and retained-state recovery. `e2e/workspace.spec.ts` checks real model
runs, saved reloads, unchanged controls, delayed freshness, mobile views, and
service/session/result failures. Set `UPDATE_SCREENSHOTS=1` to refresh the eight
application screenshots during that workflow.

For a desktop with Orca installed, start it with `orca --debug-file=orca.log` and
repeat the keyboard workflow above. The [GNOME Orca documentation](https://help.gnome.org/orca/introduction.html)
describes its options. W3C publishes the
[WCAG 2.2 requirements](https://www.w3.org/TR/WCAG22/) and
[reflow guidance](https://www.w3.org/WAI/WCAG22/Understanding/reflow.html).
[Playwright's accessibility guide](https://playwright.dev/docs/accessibility-testing)
explains the limits of automated scans.

Remote CI, a fresh-volume start of this complete revision, and the owner's
explicitly deferred physical host sleep/wake check were not run in this UI pass.
The earlier API and Python verification remains recorded in the parent Milestone 9
design. Those suites were not repeated for these frontend changes.

## Usability follow-up, 2026-10-17

The expanded suite passed all five browser workflows in 5.9 minutes, including
Login, Guide, Alarms, Analytics, administrator account-form states and the
missing-page view. Checks covered error focus, visible labels, password-manager
hints, keyboard disclosure controls, compact navigation, retained reads and
clearing restricted data after a 403 response. Axe found no violations for the
selected rules in these tested states. The administrator browser test used
mocked responses; real API authorization passed in the integration suite.

The usability gallery passed 147 applicable screen/state/width combinations.
Account and missing-page additions passed 18 more combinations before code.
The existing 96-combination gallery still passed. All checks used Chromium.
The earlier Orca results remain historical evidence; this pass did not rerun
Orca or add other assistive-technology combinations.

The complete candidate also passed its isolated fresh-volume startup and real
scenario smoke check. Remote CI and physical host sleep/wake did not run.
