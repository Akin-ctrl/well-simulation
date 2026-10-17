# Verified additions to the single SQL schema

Date: 2026-10-17
Status: Implemented and verified locally

## Purpose

Add new ordinary tables while preserving the user's existing historian. The
repository keeps `data/sql/schema.sql` as its single schema source, with a generated
catalog contract. New installations continue to apply that complete file. Existing
installations need an explicit, verified operation to add newly declared objects.

## Input and allowed change

An `extend` installer mode takes the previous schema source and generated contract
as temporary reference files. Obtain them from the installed commit before editing
the current source. They are not another maintained schema or a migration chain.
The current source must begin with the exact previous source text. The extension
is the remaining suffix, split by the existing SQL-aware statement parser.

Initially support only `CREATE TABLE` for new ordinary public tables and
`CREATE INDEX` or `CREATE UNIQUE INDEX` on those new tables. Require simple
unquoted identifiers. Reject changes to existing declarations, data statements,
ALTER, DROP, function replacement, triggers, inherited/partitioned tables,
concurrent indexes and indexes on old tables. No automatic conversion of a
changed column or existing object is part of this operation.

## Verification and transaction

1. Hold the existing installer advisory lock.
2. Load both contracts against their exact source checksums. Require identical
   engine versions and legacy-history definitions.
3. Verify the installed previous identity, history and complete catalog. If the
   target is already installed, perform the normal check and return successfully.
4. Verify the target contract retains every old catalog definition. Validate the
   suffix declarations and ensure each named table/index is new.
5. Apply only the permitted additions in one transaction with bounded lock and
   statement timeouts. Existing tables keep their object identities and rows.
6. Compare the complete resulting catalog with the target generated contract.
   A mismatch rolls back the additions.
7. Update schema-installation identity and record a successful extension audit
   event in the same transaction. Provision the explicit service grants afterward.

The new contract comes from applying the complete current SQL to a fresh database
on the pinned engine. It never comes from approving an unexplained changed live
catalog. Installation bookkeeping remains outside the application catalog and
retains its original init/adopt method and legacy history.

## Failure review

| Failure | Required result |
| --- | --- |
| Wrong previous source or contract | Refuse before DDL |
| Existing catalog drift | Refuse and keep the previous identity |
| Existing source declaration changed | Refuse; this mode only supports additions |
| DML, ALTER, DROP or old-table index in suffix | Refuse before DDL |
| New object name already exists | Refuse before DDL |
| Lock timeout or statement failure | Roll back every addition |
| Resulting catalog differs from generated target | Roll back additions and identity |
| Audit insert fails | Roll back additions and identity |
| Repeat after successful extension | Check the target and leave data unchanged |
| Service grant provisioning fails afterward | Structural identity remains valid; rerun provisioning |

## Alternatives

Replaying the whole file would collide with existing objects and could rerun
policies. Rebuilding the volume would discard data. Numbered migrations contradict
the chosen maintenance workflow. General automatic schema diffing would need to
infer data conversions and destructive intent. This bounded addition path fits
the new schedule and alarm lifecycle tables while keeping those concerns explicit.

## Design checks and verification

Check 1 read schema_install, schema_engine, schema_sql, schema_catalog and the
existing lifecycle tests. It confirmed source-bound contracts, audited identity
changes, parser handling for quoted semicolons, and the advisory installer lock.
Check 2 traced each failure above through a single transaction and checked that
no supported declaration can target an old table. Old catalog rows must remain
present; the final catalog must match the freshly generated reference exactly.

Tests must exercise real transactional rollback, repeat execution, drift refusal,
source-prefix refusal, parser rejection and preservation of data, old object IDs
and legacy history. Take a private backup and verify a restored copy before
extending the main local database. Report the actual restore and comparison
results. Keep the original volume throughout.

## Verification on 2026-10-17

The extension, installer and transaction tests passed 26 checks. They cover
unsupported SQL, changed prefixes, quoted semicolons, catalog mismatch rollback,
audit failure rollback, repeat execution and preservation of existing rows,
object identities and legacy history. A fresh reference database generated the
new contract, which contains 46 application relations.

A private 84,737,661-byte backup restored successfully into a disposable database.
Before/after row counts and row-content hashes matched for 3,511,963 existing
records across 24 tables, including the legacy ledger. The restored copy accepted
the extension and passed the complete target contract check. Its initial setup
attempt found TimescaleDB already present in the database template; the corrected
setup reused that extension and the same backup. The rehearsal database was then
removed; the private backup remains outside the repository.

The isolated and main installations both accepted the verified extension while
retaining their existing volumes. The updated installer image also passed a
normal isolated startup check and provisioned the intended service grants.

## Restore comparison follow-up, 2026-10-17

The alarm extension restore rehearsal found that PostgreSQL flattens the nested
AND produced by forecast duration `BETWEEN` checks when it parses a dump. The
catalog comparison now recognizes only that verified whole-expression shape,
retaining every bound and divisibility operand. Other structural differences
still stop installation. The [alarm verification record](alarm-lifecycle.md)
describes the finding, narrow comparison rule and regression checks. Keep using
the exact previous SQL and contract; do not edit the old schema to match a dump.

Use serial `pg_restore` for the full TimescaleDB archive. Parallel restore can
load extension catalog rows before their dependencies. The disposable rehearsal
reproduced this failure, and the [official procedure](https://github.com/timescale/Tiger-Data-Docs/blob/main/src/content/docs/deploy/self-hosted/backup-and-restore/logical-backup.mdx)
explicitly excludes `-j`. Keep `timescaledb_pre_restore()` and
`timescaledb_post_restore()` around the restore as documented.
