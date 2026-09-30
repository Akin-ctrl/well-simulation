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

   This step is currently blocked in the production image. The page returns
   HTTP 200 but stays blank because its Content Security Policy blocks the
   React Router startup scripts. Do not present the later UI steps as verified
   until a browser can render them.

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
   Readings are random values based on configured ranges. Pressure does not
   cause flow to change. The two-minute and five-minute chart buckets and
   alarm thresholds are chosen for a short demo, not a real plant.
   ```

7. Explain the target what-if scenario:

   ```text
   Close choke on WH-001 from 80% to 50%.
   ```

8. Expected future behavior:

   - flow rate decreases
   - tubing pressure rises
   - forecast confidence may decrease if operating state becomes unstable
   - alarms may trigger if thresholds are crossed

9. Inspect the data layer:

   - raw readings in TimescaleDB
   - alarm events
   - future model state snapshots
   - future forecast/scenario outputs

10. Inspect architecture decisions:

   ```text
   docs/adr/
   ```

## Current Status

The overview, analytics, per-wellhead detail pages, Alarm Center, live telemetry,
and active alarm data are implemented. Stateful twin behavior, predictions, and
what-if scenarios are still target capabilities, not current features.
