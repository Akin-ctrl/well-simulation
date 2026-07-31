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
   http://localhost:8082
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
   - active alarms on affected wellheads

5. Inspect active alarms:

   - high pressure
   - high temperature
   - high water cut
   - high vibration or degradation indicators

6. Explain the next implementation slice:

   ```text
   The next dashboard page is an Alarm Center for active alarms, severity summaries,
   threshold context, alarm age, and links back to wellhead details.
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

The overview, analytics, per-wellhead detail pages, live telemetry, and active alarm data are implemented. The Alarm Center is the next dashboard feature. Stateful twin behavior, predictions, and what-if scenarios are still target capabilities, not current implementation.
