# ADR 0039: Record acknowledgement and timed shelving separately from alarm conditions

- Status: Accepted
- Date: 2026-10-17
- Implementation: Implemented and verified locally
- Decision: Preserve the existing alarm evaluator. Store acknowledgement for one
  exact event, and timed shelving for one well/rule pair, with atomic action
  receipts and audit events. Only OPERATIONS may perform these actions.

## Context

ADR 0032 delivered active alarm inspection and required an explicit lifecycle
design before adding operator actions. The owner has now authorized that work.
An acknowledgement means someone has recognized an event. It does not establish
that the condition has cleared. A temporary shelf must survive repeated activations
of the same condition and expire without relying on a running cleanup worker.

The evaluator already debounces opening and clearing and prevents duplicate open
events for a well/rule pair. Its event identity contains a PostgreSQL bigint and
a timestamp with microseconds. JavaScript numbers and Date objects cannot preserve
both parts exactly. The API's current role grants only reads on those events.

## Options considered

1. Add flags to the alarm event and change the evaluator. This couples operator
   actions to process truth, complicates retention and needs existing-table changes.
2. Store actions only in the general audit JSON. This leaves revision checks and
   current shelving state without a clear authoritative record.
3. Add ordinary acknowledgement, shelving and action-receipt tables. This keeps
   event truth intact and lets PostgreSQL enforce unique acknowledgements and
   serialize shelf revisions. Choose this option.

## Decision

Acknowledgement belongs to the exact `(event_id, triggered_at)` pair. Shelving
belongs to `(wellhead_id, alarm_rule_id)` and lasts from 5 to 480 whole minutes.
Expiry uses the database clock on reads. An operator can end a shelf early or
explicitly replace its deadline after reviewing the current revision.

OPERATIONS uses the existing `alarm:acknowledge` capability for all three actions.
ADMIN keeps administrative authority and does not inherit operating authority.
Every write requires a reason, explicit confirmation and an idempotency key.
The transaction rechecks current account authority, changes lifecycle state,
records an immutable receipt and writes the audit event together. Failure of
any part rolls back all of them. Reusing a key with different inputs fails.

Active totals continue to count every open event, including acknowledged or
shelved events. Needs acknowledgement includes retained, unacknowledged events,
including events that have returned to normal, except while their rule is shelved.
A separate shelf list exposes every current shelf even when no event is active.
History preserves process state and operator actions as separate information.

The single canonical SQL file gains only new ordinary tables and indexes. The
verified extension procedure preserves existing objects and rows. Lifecycle
records do not reference the retained event hypertable, so they do not hold its
chunks in storage. Action receipts and acknowledgement records remain durable
until a separately designed archival policy replaces that decision.

## Consequences

Operators can distinguish recognition, process recovery and temporary shelving.
A lost HTTP reply has an exact receipt lookup. Concurrent shelf edits fail on a
revision conflict. A shelf expires even if the API stops and later restarts.
The lifecycle tables add durable operational metadata and another bounded API
transaction; they do not actuate the model or change alarm rules.

The design follows the acknowledgement and timed-shelving concepts in the public
OPC Foundation reference. It does not implement an OPC UA server or claim complete
ISA-18.2/IEC 62682 conformance. A complete site alarm program also needs an alarm
philosophy, rationalization, operator procedures and ongoing performance review.
Revisit this decision for physical plant use, independently approved shelving,
site-specific maximum durations or a durable alarm archive beyond current retention.

## References

- [Detailed design and verification](../design/alarm-lifecycle.md)
- [ADR 0032](0032-alarm-center-scope.md)
- [OPC Foundation definitions](https://reference.opcfoundation.org/specs/OPC-10000-9/3.1)
- [Acknowledgement identity and comment](https://reference.opcfoundation.org/specs/OPC-10000-9/5.7.3)
- [Timed shelving](https://reference.opcfoundation.org/specs/OPC-10000-9/5.8.17.4)
- [IEC 62682 scope](https://webstore.iec.ch/en/publication/65543)
