# Demo Script

## Goal

Show a reviewer how one synthetic well moves from live monitoring to a saved
what-if comparison. The short route is Overview, Monitoring, Scenarios, then
Saved runs. Allow roughly five minutes after the stack is ready.

## Start and sign in

Follow the root README to create the local environment, then run:

```bash
docker compose up -d --build --wait
```

Open <http://localhost:8090> and sign in as demo@example.com with the documented
local password, demo-operator-2026. This USER account can save model analyses
but cannot change installed controls.

The Guide in the main navigation explains each view and includes a first-run
walkthrough. Contextual help beside the forms explains their inputs.

## Walk through one well

1. In Overview, find WH-001. Read pressure, flow, water cut, model status, and
   conditions. The Needs attention filter includes signals outside those three
   headline readings. A configured well remains visible when it has no data.
2. Open the well's Monitoring view. Check readings, then expand Model
   diagnostics to compare sample time with process time. The scheduler tick appears
   separately because a ticking service does not prove a successful calculation.
3. Read Installed controls. Note the choke opening and revision before running
   the scenario. This panel has no live command buttons.
4. Open Scenarios. Name the run Choke comparison, choose a five-minute duration
   and a 60-second chart interval, and set the first change to minute zero. Select Choke
   opening and enter 50 percent. If the current choke is already 50 percent,
   choose a different target so the comparison shows a change.
5. Select Run scenario. The model runs from a copy of current state and saves
   its result. Read the named saved result and its generation time on the right.
6. Compare Flow rate and Tubing pressure using the signal selector. The dashed
   line holds the copied controls unchanged; the purple line follows the proposed
   schedule. Each chart uses the signal's canonical unit. The difference is
   scenario minus baseline at the same final sample.
7. Expand Original run inputs and the plotted-value table. Changing the form's
   draft does not change these saved inputs. Confidence reasons explain the
   model's limits. The heuristic score is not a probability. Threshold results
   describe returned samples and can miss events between them.
8. Open Saved runs and reopen the named result. Refreshing that result's URL
   reads saved points without running another scenario. Other authorized
   dashboard readers can also read it.
9. Return to Monitoring and confirm the installed controls and revision are
   unchanged. The synthetic process continues evolving while analysis runs.

The Forecast view follows the same pattern with no control schedule. It holds
the copied controls constant. Both forms accept five to 120 minutes. Longer
runs reduce the model's heuristic confidence.

## Explain the limits

All 18 signals come from a synthetic model. Pressure, flow, and temperature
respond together; simpler diagnostic equations estimate gas, sand, corrosion,
annulus pressure, and vibration. No field measurements calibrated these
equations. Current trend buckets and alarm thresholds suit a short demo.

The Alarm Center and monitoring charts remain available from the navigation.
The present operating point may have no active alarms. Do not claim alarms
exist when the page reports none. Projected threshold results do not create
live alarm events.

A saved result may expire under retention while its history entry remains.
A timeout can leave the submission outcome unknown. Use Saved runs to look for
the result before submitting again. The application does not retry the POST.

## Technical follow-up

- Open the architecture overview for the model, Modbus, ingestion, and historian path.
- Open the Milestone 9 design for screen logic, failure handling, and verification.
- Open the API specifications for forecast, scenario, status, and history contracts.
- An OPERATIONS account can open Controls, save a proposal, review it and activate
  it. Check the command outcome and running revision separately. An ADMIN account
  uses Parameters to publish an override; existing models need a service restart.
  The [operating workspace design](../design/operating-workspace.md) records both flows.
  Field calibration and physical control remain outside this demo’s implemented scope.

## Compare an earlier state

Open Scenarios and select Saved snapshot. Search for a time in the displayed time
zone, choose a compatible candidate, and review its exact time. Name the run,
choose a control change, and run it. Check Starting state in the result, then
reload the link to confirm that it reopens the same saved branch. The model uses
alarm rules active at execution. Older branches can lose chart points sooner
because retention follows simulated dates.

## Inspect and acknowledge an alarm

Open Alarm Center and choose a well. Needs acknowledgement can include an event
whose condition already returned to normal. All active counts every open
condition, including acknowledged and shelved ones. Expand the help section
for those distinctions and the event details for its exact identity.

With an OPERATIONS account, select Review action, read the target and effect,
enter a reason and confirm. Acknowledgement records recognition; normal readings
clear the condition. Shelve rule sets a temporary deadline for that well and
rule, including later events before the deadline. Shelved rules shows the reason
and deadline even when no event is active. End shelf now returns unacknowledged
events to the attention view. If a reply is lost, use Check result before any
explicit retry. Do not create an alarm just to populate the presentation.
