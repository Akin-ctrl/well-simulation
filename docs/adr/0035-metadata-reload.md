# ADR 0035: How Metadata Changes Reach A Running Service

- Status: Accepted
- Date: 2026-08-29
- Implementation: Implemented. All three services re-read metadata on a timer. The gateway clears its register image.

## Context

The system is metadata-driven. Wellheads, parameter types, Modbus register
mappings, and alarm rules all live in database tables, and the three Python
services configure themselves from those tables.

They do it once. `modbus_gateway.py`, `database_ingestion.py`, and
`wellhead_simulator.py` each call their loader inside `main()` and cache the
result for the life of the process. Editing a row changes nothing until the
container restarts.

`data/README.md` says otherwise. It claims a Modbus address can be changed with
a simple UPDATE, and that alarm rules can be defined "on the fly without
restarting any services". Both are false today.

ADR 0034 adds an audit trail that records config changes. That makes this worse
rather than better: a row saying someone changed an alarm rule reads as though
the change took effect, when it did not.

Two things have to be true together. A change must actually reach the running
service, and the record of that change must mean what it appears to mean.

## Options Considered

### Option A: Do nothing, and correct the documentation

Delete the hot-reload claim. Say plainly that a restart is required.

- Pros: no code, and it stops the docs lying.
- Cons: the metadata-driven design is the thing this project is demonstrating.
  Requiring a container restart to change an alarm threshold undercuts it. A
  reviewer would reasonably ask why the tables exist.

### Option B: Re-read on a timer

Each service reloads its metadata every N seconds.

- Pros: simple, no new endpoints, no database features, works the same in every
  service.
- Cons: there is always a staleness window. Every service queries the database
  on every tick whether anything changed or not, which is wasted work at any
  scale beyond this one.

### Option C: Listen for a Postgres notification

A trigger on the metadata tables issues `NOTIFY`. Each service holds a
`LISTEN` connection and reloads when it hears one.

- Pros: changes land in about a second. No polling. The database is already the
  source of truth, so the signal comes from the right place.
- Cons: a listener thread in each service, a second database connection held
  open, and triggers on four more tables. A dropped connection means missed
  notifications unless the reconnect also re-reads, which is easy to get wrong.

### Option D: A reload endpoint on each service

Each service exposes `POST /reload`. Something calls it after a config change.

- Pros: explicit and easy to reason about. Nothing happens that was not asked
  for, which suits a system that will later accept control commands.
- Cons: something has to know which services to call and what to do when one
  fails. That orchestration does not exist yet. The services currently have no
  HTTP surface at all, so this means adding one to all three.

## Rationale

Option B is the only one that adds no new surface. Option C
adds a listener thread and triggers, Option D adds an HTTP server to three
services that currently have none, and both are more machinery than a 12
wellhead simulation needs to prove the point. A staleness window of one
telemetry interval is not meaningful when the telemetry itself is synthetic.

Option C becomes the right answer if change latency ever matters, which it would
in a real plant. Option D becomes the right answer once the control API exists,
because by then the services have an HTTP surface anyway and explicit is better
than implicit for anything that changes plant behaviour.

## Reload And The Register Image

One behaviour has to be decided alongside the reload itself.

The Modbus gateway holds a register image built from the current mappings. If a
mapping moves from register 100 to register 200 and the gateway reloads, the old
value is still sitting at register 100. Ingestion will not read it, because
ingestion reloaded too, but it is stale data being served on a live interface.

Three ways to handle it:

- Clear the whole register block on reload. Simple. Every value reads as zero
  until the next telemetry batch, which is a visible gap.
- Clear only the registers whose mappings changed. Precise, and the bookkeeping
  is easy to get wrong.
- Leave it. Nothing reads those registers, so nothing breaks.

Leaving it contradicts the freshness work in Phase 1, which exists so that no
part of this system serves values that look current and are not.

**The whole block is cleared on reload.** A visible gap of one telemetry
interval is the honest outcome. A stale value sitting on a live Modbus
interface is not, and clearing only the registers that moved is the kind of
bookkeeping that fails silently, which is the same class of bug as F-2.

## Decision

**Re-read metadata on a timer**, at the telemetry interval, in all three Python
services. Clear the gateway's register block on every reload that changes the
mappings.

## Consequences

- A change to a wellhead, register mapping, or alarm rule takes effect within
  one telemetry interval. The claim in `data/README.md` becomes true, so the
  wording there is corrected rather than deleted.
- Each service issues one extra query per interval. At 5 seconds and three
  services that is negligible, and it is the cost that makes Option C
  attractive if the service count ever grows.
- The reload has to be safe to run mid-loop. The gateway rebuilds its register
  store, so the load and the swap happen together rather than leaving a
  half-updated map visible.
- Ingestion skips one cycle after a mapping change, because the registers it
  would read have just been cleared. That is correct: there is nothing fresh to
  record yet.
- A `config.change` audit row from ADR 0034 now means the change reached the
  running system, within one interval.

## What Would Make Us Revisit

- Change latency starts to matter, which pushes toward Option C.
- The number of services grows enough that polling cost is real.

## Amendment, 2026-08-29

Option D was rejected partly because the services had no HTTP surface and
adding one to all three was more machinery than the problem deserved.

Phase 8 added that surface anyway. Each service now serves `/health`, `/ready`,
and `/metrics` on its own port, because ADR 0017 requires health and readiness
and ADR 0029 requires metrics. A reload endpoint would now be a handler on a
server that already exists.

The decision stands, but for one reason rather than two. Polling costs one
query per service per interval, which is nothing at this size, and a reload that
happens without anyone asking is simpler to reason about than one that depends
on something remembering to call it.

That balance changes when the control API ships. At that point a config change
becomes an operator action that should take effect when the operator says so,
not up to an interval later, and it needs the audit trail from ADR 0034 around
it. Revisit Option D then.
