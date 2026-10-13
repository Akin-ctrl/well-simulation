# Demo Script

This script describes the intended portfolio-review flow.

## Goal

Show that the project is an industrial wellhead monitoring simulation evolving into a lightweight digital twin foundation.

## Flow

1. Start the local stack:

   ```bash
   docker compose up --build
   ```

2. Open the dashboard:

   ```text
   http://localhost:8090
   ```

3. Confirm live telemetry:

   - wellhead pressure
   - flow rate
   - temperature
   - water cut
   - valve/pump status

4. Inspect the current implemented dashboard:

   - fleet overview
   - analytics charts
   - per-wellhead detail page
   - Alarm Center and active alarms on affected wellheads

5. Inspect active alarms:

   - high pressure
   - high temperature
   - high water cut
   - high vibration or degradation indicators

6. Explain what is simulated:

   ```text
   All 18 signals come from a synthetic process model. Pressure, flow, and
   temperature respond together; diagnostic equations estimate gas, sand,
   corrosion, annulus pressure, and vibration. No field data calibrated these
   equations. Two-minute and five-minute chart buckets and alarm thresholds
   are chosen for a short demo.
   ```

7. Use the authenticated API to run a private what-if scenario:

   ```text
   POST /api/twin/scenarios
   Choose an active well, a 300-second horizon, and a control schedule that
   closes its choke to 50 percent at minute zero. Use the request shape in
   docs/openapi/public-api.yaml. The response includes the saved run ID and
   baseline values for comparison. The dashboard has no scenario form yet.
   ```

8. Inspect the scenario response:

   - Compare flow and pressure with the baseline, which holds current controls.
   - Read the heuristic confidence score and its reasons. It is not a measured probability.
   - Inspect projected threshold crossings. These do not create live alarms.
   - Confirm the running well's controls are unchanged by the scenario.

9. Inspect the data layer:

   - raw readings in TimescaleDB
   - alarm events
   - model state snapshots and restart recovery events
   - saved forecast and scenario runs and their points

10. Inspect architecture decisions:

   ```text
   docs/adr/
   ```

## Current Status

The overview, analytics, per-wellhead detail pages, Alarm Center, live telemetry,
and active alarm data are implemented. The backend also implements stateful
synthetic wells, snapshots, saved forecasts, and isolated scenarios. Live
synthetic controls use the Milestone 8B draft and activation API. Forecast and scenario
dashboard features remain deferred.
