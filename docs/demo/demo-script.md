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

4. Inspect active alarms:

   - high pressure
   - high temperature
   - high water cut
   - high vibration or degradation indicators

5. Run or inspect a what-if scenario:

   ```text
   Close choke on WH-001 from 80% to 50%.
   ```

6. Expected behavior:

   - flow rate decreases
   - tubing pressure rises
   - forecast confidence may decrease if operating state becomes unstable
   - alarms may trigger if thresholds are crossed

7. Inspect the data layer:

   - raw readings in TimescaleDB
   - model state snapshots
   - forecast/scenario outputs
   - alarm events

8. Inspect architecture decisions:

   ```text
   docs/adr/
   ```

## Current Status

This is the target demo flow. Some pieces still need implementation and integration before the full flow works end-to-end.

