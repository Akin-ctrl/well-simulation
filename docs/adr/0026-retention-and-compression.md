# ADR 0026: Use Tiered Retention And TimescaleDB Compression

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Installed local policies recorded in the 2026-10-17 amendment

The original policy table below records the proposed targets. Use the
[current policy amendment](#amendment-2026-10-17-record-the-installed-retention-policy)
for this deployment's actual retention and compression.

## Context

The system will generate raw readings, alarm events, model state snapshots, prediction points, scenario points, control commands, and recovery events. These data classes have different volume, value, and audit requirements.

Keeping everything forever is not production-grade. Expiring everything quickly would weaken operational history and auditability.

## Options Considered

### Option A: Keep everything forever

- Pros: Simple and avoids accidental data loss.
- Cons: Unbounded growth and poor lifecycle management.

### Option B: Use short retention for everything

- Pros: Minimizes storage.
- Cons: Loses important audit and operational history.

### Option C: Use tiered retention by data value and volume

- Pros: Practical, production-grade, and preserves high-value records longer.
- Cons: Requires explicit policy management.

## Decision

Use **Option C: tiered retention and TimescaleDB compression policies**.

## Default Policy

| Data | Retention | Compress After | Reason |
| --- | ---: | ---: | --- |
| Raw readings | `180 days` | `7 days` | Core historian data |
| Alarm events | `2 years` | `30 days` | Operational audit |
| Model state snapshots | `90 days` | `7 days` | Recovery and model analysis |
| Prediction points | `30 days` | `3 days` | Forecast points become stale |
| Scenario points | `90 days` | `7 days` | Useful for review and demonstration |
| Control commands | `3 years` | optional | Long-lived audit trail |
| Recovery events | `2 years` | optional | Reliability audit |

## Rationale

High-volume time-series data should be compressed and expired earlier. Audit-oriented data such as control commands, alarms, and recovery events should live longer because it explains operational decisions and system behavior.

## Environment Overrides

Retention should be configurable by deployment environment:

- local/demo environments may use shorter retention
- production environments may use longer retention
- regulated environments may require policy-specific retention

## Consequences

- SQL migrations should include TimescaleDB compression and retention policy setup where appropriate.
- Scenario metadata may outlive detailed scenario points.
- Operators should understand that forecast points are not permanent historical truth.
- Retention policies must be documented in deployment docs.

## Implementation on 2026-10-12

Migration 0010 gives `twin_state_snapshot` a 90-day retention policy and
compression after 7 days. `twin_recovery_event` has a 2-year retention policy.
The prediction, scenario, and control rows in the table above remain plans for
later milestones.

## Implementation on 2026-10-13

Migration 0011 compresses prediction points after 3 days and retains them for
30 days. Scenario points are compressed after 7 days and retained for 90 days.
Run metadata remains in ordinary tables and outlives its points; the API
returns HTTP 410 when any saved point has expired. The control-command policy
is still planned for Milestone 8B.

## Amendment 2026-10-17: Record the installed retention policy

### Context and decision

The original targets above do not describe the local schema. Milestone 8B also
deliberately retained command history without automatic expiry. Scheduled jobs
and alarm actions added more durable ordinary tables. This amendment records
the implemented policy from `data/sql/schema.sql`; it changes no database policy.

| Stored data | Automatic retention | Compression after |
| --- | --- | --- |
| `parameterreading` | 7 days | 1 day |
| `alarmevent` | 90 days | 7 days |
| `auditevent` | 90 days | None configured |
| `twin_state_snapshot` | 90 days | 7 days |
| `twin_recovery_event` | 2 years | None configured |
| `prediction_point` | 30 days | 3 days |
| `scenario_point` | 90 days | 7 days |
| `prediction_run`, `scenario_run` | No automatic expiry | Not configured |
| `control_command`, `control_command_event`, `model_parameter_change` | No automatic expiry | Not configured |
| `wellhead_control_state`, `wellhead_model_parameter` | No automatic expiry | Not configured |
| `forecast_schedule`, `forecast_job` | No automatic expiry | Not configured |
| `alarm_action`, `alarm_acknowledgement`, `alarm_shelf` | No automatic expiry | Not configured |

Retention policies remove eligible time chunks when their background jobs run;
these ages are not exact per-row deletion deadlines. Projection point retention
uses simulated time. A newly calculated historical scenario can therefore lose
points sooner than one starting now. The API returns 410 when a saved run no
longer has its complete point set, while run metadata and job receipts remain.

Continuous aggregates have refresh policies, but no separate automatic retention
policy. A refresh window is not a retention guarantee. Metadata tables also have
no age-based expiry. Explicit asset deletion and foreign-key actions are separate
from the time-based policies in this table.

There is no environment-variable interface for changing these policies. A future
policy change must update the canonical SQL and provide a reviewed conversion for
existing installations. Do not reintroduce numbered schema files.

### Consequences

**Good.** Short demo retention bounds the largest histories. Durable receipts keep
the meaning of reviewed operating actions after general audit rows expire.

**Costs.** Ordinary history and aggregate storage can grow. A future archive
policy must account for references between commands, results and receipts.

**Accepted risks.** This local deployment has no automatic archive for those
records. Do not infer three-year command expiry or permanent alarm-event storage
from the original targets. A longer-lived installation needs an explicit archive
and capacity plan.

### Related

- [ADR 0038](0038-live-control-command-lifecycle.md): durable control history.
- [ADR 0039](0039-alarm-operator-actions.md): acknowledgement survives event retention.
- [Scheduled forecasts](../design/scheduled-forecasts.md): job records and expiring points.
- [Database updates](../operations/database-updates.md): verified changes to an existing installation.

