# Update an existing database

Checked against application commit `82c9baf`, 2026-10-17.

The project maintains one schema, `data/sql/schema.sql`. An existing database
also records which source and generated contract it matches. The contract is a
comparison record of tables, functions and policies. It is not another schema.

Choose the path below before starting a newer application against existing data.
The installer refuses a mismatch instead of guessing a conversion.

## Choose the starting point

| Installed state | Next operation |
| --- | --- |
| Empty database on the current engine | Normal Compose startup installs the complete current schema |
| Legacy numbered history, no `schema_installation` | Adopt with the installer, SQL and contract that match that legacy structure and engine |
| Adopted PostgreSQL 14 / TimescaleDB 2.19.3 baseline | Verify physical recovery, convert a copy, then accept the engine with unchanged schema source |
| PostgreSQL 16.15 / TimescaleDB 2.30.2 baseline missing only the new tables | Run the current `extend` mode with its exact previous SQL and contract |
| Current baseline and engine | Normal startup checks it; `check` performs a read-only verification |
| Unknown history, changed existing definitions or an incomplete installation | Inspect the mismatch and prepare a specific conversion or recovery plan |

Do not run today's `adopt` against the original 13-file legacy installation.
The current schema includes five later tables that legacy history never created.
Adoption checks structure; it does not add those tables. Likewise, `accept-engine`
requires identical SQL source before and after the engine change. It cannot also
accept new application tables.

## Prepare and identify the installation

Keep a verified backup and compare a restored copy before changing the main
installation. Use the [physical recovery procedure](../design/consolidated-schema.md#repeating-the-physical-backup-check)
for the affected old engine. Use the [complete logical restore procedure](../design/database-engine-upgrade.md#restoring-a-logical-backup-completely)
after the engine upgrade, including roles and database grants. Restore serially,
with the TimescaleDB pre/post restore calls; do not use `pg_restore --jobs`.

Record the engine versions, legacy ledger and, when present, the source and
contract checksums in `schema_installation`. Compare them with the candidate
reference. A commit name alone does not prove the database matches it. An
interrupted installation needs recovery before any of these paths can continue.

These historical references are available in Git:

| Reference | Meaning |
| --- | --- |
| `cd09691` | Consolidated legacy schema on PostgreSQL 14 / TimescaleDB 2.19.3 |
| `e150992` | The same schema source on PostgreSQL 16.15 / TimescaleDB 2.30.2 |
| `54576c0` | Adds `forecast_schedule` and `forecast_job` |
| `6631dd4` | Same database schema as `54576c0`, before alarm lifecycle tables |
| `82c9baf` | Adds `alarm_action`, `alarm_acknowledgement` and `alarm_shelf` |

An older numbered ledger must contain every expected legacy checksum before
adoption. Unknown or earlier partial histories need their own reviewed path.
Do not manufacture ledger rows or edit checksums to make a check pass.

## Command environment

Run the examples from the repository root in Bash. Activate a Python 3.12 virtual
environment with `requirements-dev.txt` installed. Install the pnpm dependencies
under `monorepo/` as described in its README. Commands below use its dotenv loader
to read the ignored root `.env`, without printing credentials.

`POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB` must identify the database
administrator and database being verified. Service provisioning also uses the
configured `TWIN_DB_*` and `API_DB_*` values. Set the host and port for the
restored copy first:

```sh
schema_db_host=127.0.0.1
schema_db_port=55440
schema_repo_root=$(pwd)

schema_python() {
  (
    cd "$schema_repo_root/monorepo" || exit 1
    pnpm exec dotenv -e ../.env -- env \
      POSTGRES_HOST="$schema_db_host" POSTGRES_PORT="$schema_db_port" \
      python3 "$@"
  )
}
```

Port 55440 is the recovery example, not an automatically created server. Confirm
the target before running a command. After the rehearsal, stop application
writers and take the final backup before repeating the verified operation on
the main installation. Its default host database port is 5434.

## Adopt the original legacy structure

Export the matching historical files into a temporary directory. This leaves
the current branch untouched and supplies the correct installer and SQL together:

```sh
schema_legacy_dir=$(mktemp -d /tmp/well-schema-legacy.XXXXXX)
git archive cd09691 | tar -x -C "$schema_legacy_dir"
schema_python "$schema_legacy_dir/data/src/schema.py" adopt --without-seeds
schema_python "$schema_legacy_dir/data/src/schema.py" check
```

This example applies only to the complete legacy structure on its original
PostgreSQL 14 / TimescaleDB 2.19.3 engine. It verifies all 13 legacy files and
the actual catalog, then records the baseline without replaying SQL or seeds.
It also provisions configured service grants. A database already adopted at
this baseline needs only its matching `check` before the engine procedure.

## Convert the engine without adding tables

Follow the [engine conversion design](../design/database-engine-upgrade.md) on
copies of the verified old cluster. Keep the original volume. That procedure
requires binary conversion, extension update, record and grant comparisons,
logical restore verification and application checks.

Use the `e150992` installer and contract for engine acceptance, because its SQL
matches `cd09691`. Run this after binary conversion, extension update and the
record, permission and catalog comparisons pass on the copy:

```sh
schema_engine_dir=$(mktemp -d /tmp/well-schema-engine.XXXXXX)
git archive e150992 | tar -x -C "$schema_engine_dir"
schema_python "$schema_engine_dir/data/src/schema.py" accept-engine \
  --previous-contract "$schema_legacy_dir/data/sql/schema-contract.json"
schema_python "$schema_engine_dir/data/src/schema.py" check
```

For an already adopted source, export `cd09691` as above to obtain its contract
without running `adopt` again. Point `schema_db_host` and `schema_db_port` at the
converted server before accepting it. Engine acceptance changes the recorded
contract identity and adds its audit event; it does not install tables or grants.
Do not substitute the current five-table extension's contract at this stage.
Complete the logical restore and application checks before changing the main
installation, following the order in the engine design.

## Add the current ordinary tables

Select the commit whose SQL and contract match the installed PostgreSQL 16
baseline. The example starts immediately after engine acceptance. Use `54576c0`
or `6631dd4` instead if that installation already has scheduled forecasts.

```sh
schema_installed_commit=e150992
schema_previous_dir=$(mktemp -d /tmp/well-schema-previous.XXXXXX)
git show "$schema_installed_commit:data/sql/schema.sql" > "$schema_previous_dir/schema.sql"
git show "$schema_installed_commit:data/sql/schema-contract.json" > "$schema_previous_dir/schema-contract.json"
schema_python "$schema_repo_root/data/src/schema.py" extend \
  --previous-schema "$schema_previous_dir/schema.sql" \
  --previous-contract "$schema_previous_dir/schema-contract.json" \
  --without-seeds
schema_python "$schema_repo_root/data/src/schema.py" check
```

The target SQL must begin with the exact previous source. The installer accepts
only new ordinary tables and their indexes, verifies the resulting catalog and
records the new identity with an audit event in one transaction. It provisions
service grants afterward. Existing definitions and stored readings remain intact.
The [extension design](../design/schema-extension.md) lists every refusal case.

Normal startup can proceed after the main installation passes the same checks.
Set `TIMESCALE_DATA_VOLUME` to the verified converted volume when an engine change
required one. Confirm readiness, current readings, stored controls and service
permissions after starting the current application. Keep the backup and reference
files with the deployment record; they are not new maintained schema files.

## Verification scope

The consolidation, engine upgrade, scheduled extension and alarm extension each
have dated verification records in their linked designs. This guide brings their
order together. The documentation update checks source identities and command
interfaces; it does not claim a new end-to-end legacy upgrade rehearsal.
