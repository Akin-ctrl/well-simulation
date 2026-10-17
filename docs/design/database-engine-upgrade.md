# Upgrade the database engine and verify recovery

Date: 2026-10-16

## Problem and target

The schema consolidation exposed a restore failure in TimescaleDB 2.19.3.
Compressed columns containing null values can be dumped but cannot be restored.
Our historian failed at the model_version column of a compressed chunk.
The successful physical recovery check remains the starting recovery method.

The target is PostgreSQL 16 with TimescaleDB 2.30.2. Both the original image
and target image are identified by a digest, which fixes their exact contents.

| Purpose | Image digest, under timescale/timescaledb |
| --- | --- |
| Original PostgreSQL 14 / TimescaleDB 2.19.3 | sha256:0f5553d37abbe0f20bfe0a04dab5d1238955b5cda2711aba403090f95b6e51ff |
| Target PostgreSQL 16 / TimescaleDB 2.30.2 | sha256:6f139d56042989bd35f50ba5986e492cd32e35cc6778c50be2659add298dc09f |

The vendor support matrix includes PostgreSQL 16 for both extension versions.
PostgreSQL 14 cannot run the extension version containing the fix. PostgreSQL 16
therefore provides an intermediate engine that can run the old extension before
updating it. The TimescaleDB release selected here was published on 2026-09-29.

## Options considered

**Upgrade PostgreSQL using copies of its database files, then update TimescaleDB
(chosen).** PostgreSQL's pg_upgrade tool carries the data to a new major version.
It avoids decoding the broken compressed values during a logical restore.
The vendor maintainer recommends this sequence for the reported defect.
It requires both sets of PostgreSQL binaries and space for separate volumes.

**Decompress the old database before a logical backup.** This could avoid the
faulty compressed format, but would rewrite the historian and require more disk
space. It also needs a complete recompression and policy check afterward.

**Keep PostgreSQL 14 and physical backups.** This is the verified fallback.
It leaves the logical restore failure unresolved and prevents current
TimescaleDB updates. It is not the long-term target.

## Design and sequence

1. Save the original generated schema contract outside the source tree.
2. Take a new physical backup using pg_basebackup. Verify its manifest using
   pg_verifybackup. Preserve this backup unchanged.
3. Recover a separate copy with the original image. Disable background jobs
   and application connections while comparing records.
4. Record application rows, sequence positions, role properties and grants,
   legacy schema history, and the schema installation identity.
5. Stop the recovered server cleanly. Initialize a separate PostgreSQL 16
   data volume with the same database administrator, encoding, and locale.
6. Run pg_upgrade --check, then pg_upgrade in its default copy mode. Do not use
   --link. Keep TimescaleDB at 2.19.3 during this step.
7. Start the upgraded copy with the target image. In a new connection to each
   database containing the extension, run ALTER EXTENSION timescaledb UPDATE
   TO '2.30.2'. Check template databases as well as the application database.
8. Compare all preserved records and permissions. Generate the target schema
   contract from schema.sql in a separate empty database and review differences.
9. Accept the new engine identity with the guarded installer command below.
10. Take a logical backup of the upgraded copy. Restore it into another empty
    database on the same target engine. Use timescaledb_pre_restore() before
    pg_restore and timescaledb_post_restore() afterward. Restore the archive's
    database-level grants separately, as described below. Require zero restore
    errors and compare the restored data and database structure.
11. Run schema, API, and model persistence tests against the target engine.
12. Only after the rehearsal passes, stop application writers, take a final
    copy of the cleanly stopped original cluster, and repeat the verified steps.
    Preserve the original volume and restart the services against the new one.
13. Verify through the dashboard's public /api path after the API starts.
    Older dashboard images need the reload described below. Verify readiness, current
    telemetry, persisted controls, and database permissions. Record actual results.

data/upgrade/Dockerfile builds the temporary pg_upgrade tool image. It keeps the
original binaries and their dependencies separate from the target binaries.
It runs as the postgres operating-system user. Its build arguments default to the
two verified image references. The old-binary.sh wrapper confines the older
library search path to the old server. This image is an upgrade tool, not a
replacement database service.

### Running the binary conversion

The old data directory below must be a recovered copy whose server has stopped
cleanly. The new directory must be empty. Both volumes must belong to the
postgres operating-system user. The example uses the project's admin database
role and en_US.utf8 locale; check those values on the source first.

```bash
docker build -t well-pg-upgrade:20261016 data/upgrade

old_copy_volume=well_engine_old_copy_20261016
new_volume=well_engine_target_20261016
work_volume=well_engine_upgrade_work_20261016

docker run --rm --entrypoint initdb \
  --mount "type=volume,source=$new_volume,target=/new" \
  well-pg-upgrade:20261016 -D /new -U admin \
  --encoding=UTF8 --locale=en_US.utf8 \
  --auth-local=trust --auth-host=scram-sha-256

upgrade_options=(
  -b /opt/old-bin -B /usr/local/bin -d /old -D /new -U admin
  -o '-c shared_preload_libraries=timescaledb -c timescaledb.max_background_workers=0 -c shared_buffers=128MB'
  -O '-c shared_preload_libraries=timescaledb -c timescaledb.max_background_workers=0 -c shared_buffers=128MB'
)
upgrade_mounts=(
  --mount "type=volume,source=$old_copy_volume,target=/old"
  --mount "type=volume,source=$new_volume,target=/new"
  --mount "type=volume,source=$work_volume,target=/upgrade"
)
docker run --rm "${upgrade_mounts[@]}" well-pg-upgrade:20261016 \
  "${upgrade_options[@]}" --check
docker run --rm "${upgrade_mounts[@]}" well-pg-upgrade:20261016 \
  "${upgrade_options[@]}"
```

Run the second command only after the check succeeds. The reduced shared buffer
size applies to these temporary processes. The original configuration and
authentication files are copied to the target after conversion and checked at
startup. The pg_upgrade-generated delete_old_cluster.sh is not a recovery step.
Keep the old copy until verification and retention decisions are complete.

### Restoring the browser-to-API path

The original dashboard image resolved the API service name only when nginx
loaded. Changing the API address could leave the proxy returning 502 while
its static-page health check stayed healthy.

The [ADR 0017 amendment](../adr/0017-observability-baseline.md) adds automatic
DNS refresh and checks both the page and /api/ready. Rebuild and recreate the
dashboard to use that fix. For an older dashboard image, the recovery step is:

```sh
docker compose exec -T dashboard nginx -t
docker compose exec -T dashboard nginx -s reload
```

Require the validation command to succeed before reloading. Check /api/ready
through the dashboard port, then sign in and load current readings in a browser.
A direct API health check or HTTP 200 from the dashboard's static page alone
does not establish that this path works.

### Restoring a logical backup completely

A database archive and cluster roles are separate backup artifacts. Protect both
because they contain account records or credential hashes. Use umask 077 when
creating them. The rehearsal uses pg_dump -Fc --create for the database and
pg_dumpall --roles-only for the roles. Restore into a separate server, create
the original roles, and create an empty database with the original name and
owner from template0. Install the exact TimescaleDB version first.

Run timescaledb_pre_restore(), then serial pg_restore --exit-on-error against
that database, followed by timescaledb_post_restore(). Check all exit codes.
Do not use --jobs for a full TimescaleDB restore; see the
[restore finding](schema-extension.md#restore-comparison-follow-up-2026-10-17).

The pre-created database is required by this restore sequence. pg_restore
without --create does not apply the archive's database-level grants. In our
rehearsal, this initially gave both service roles temporary-table permission
through PUBLIC, even though their table grants were correct. Generate the
archive's creation SQL with:

```sh
pg_restore --schema-only --create --file=archive-schema.sql database.dump
```

Review and apply its database ownership and database GRANT/REVOKE statements
to the restored database. For this project there are four database permission
statements: revoke PUBLIC's old CONNECT/TEMPORARY grants, grant PUBLIC CONNECT,
and grant CONNECT to twin_reader and well_api. The owner is admin. Read the
actual archive when role names differ. Verify both service credentials and
confirm TEMP and schema CREATE remain denied before starting applications.

The normal installer also reapplies the configured service restrictions. A
restoration check must verify the restrictions before declaring recovery complete.

## Accepting the generated reference

For the original consolidated baseline, use the `e150992` installer and SQL
with the previous `cd09691` contract. Their schema source is identical. Today's
schema also contains schedule and alarm tables, so it cannot perform that
engine-only acceptance directly. Follow the
[existing-database sequence](../operations/database-updates.md) to keep the
installer, source and engine reference together, then add later tables separately.

The installer rejects an engine or contract mismatch during normal startup.
Use the Bash helper and historical directories from the update guide after
verifying the converted copy's records, permissions and catalog:

```sh
schema_python "$schema_engine_dir/data/src/schema.py" accept-engine \
  --previous-contract "$schema_legacy_dir/data/sql/schema-contract.json"
schema_python "$schema_engine_dir/data/src/schema.py" check
```

POSTGRES_* must identify the database being checked. The generated contract in
the selected historical tree must describe its schema on the target engine.
The saved previous contract must match the database's recorded identity.

accept-engine runs under the existing installer lock. It requires the same
schema.sql checksum and legacy file checksums in both references. It verifies
the actual target engine and every catalog section, then updates only the
recorded contract checksum. It writes schema.engine.accepted to auditevent in
the same transaction, recording the database actor, both engine versions, both
contract checksums, and the source checksum. An audit failure rolls back the
identity change. Repeating a successful acceptance performs checks without
adding another event. It does not apply SQL, seed records, or service grants.

The command verifies database identity and structure. Backup restoration,
record preservation, and application tests remain separate release gates.
Do not use acceptance to approve an arbitrary schema edit. schema.sql remains
the only maintained SQL schema. Do not reintroduce numbered schema files.

## Failure handling and recovery

If the copy or pg_upgrade check fails, keep the original server and investigate
the isolated result. A failed extension update or catalog comparison blocks
acceptance and service startup. A failed restore blocks the live change.
No step deletes the original database volume or the verified physical backup.

Once the new database accepts writes, the old volume no longer contains the
latest readings or changes. Returning to it then requires an explicit decision
about those newer records. Do not silently switch back after writes resume.

## Verification record

The isolated rehearsal completed on 2026-10-16:

- The new physical backup passed pg_verifybackup and recovered on a separate
  volume. PostgreSQL's compatibility check and copy-mode upgrade both passed.
- All 10,165,815 original application rows, 474,202 aggregate rows, 15 sequence
  positions, role records, and effective permissions survived the engine change.
- The accepted database added one engine audit event. Its logical archive
  restored 10,165,816 application rows and all aggregate rows with matching
  content fingerprints, sequence positions, roles, and effective permissions.
- The original engine reproduced the compressed-null error. The upgraded engine
  decoded the same value, and a separate compressed 100-row null fixture passed
  a complete logical backup and restore.
- The final schema gate passed 83 tests. A later comparison self-review added a
  duplicate-entry regression, and all 15 normalization tests passed afterward.
- The full Python suite passed 249 tests; 51 database opt-ins were skipped in
  that run. All 51 ran separately: 38 schema cases and 13 model persistence cases.
- All 89 API integration tests passed using restricted database credentials.
- Ruff, formatting, strict mypy, prose checks, and the installer build passed.

The rehearsal found two engine representation differences. TimescaleDB removes
nine internal triggers and the finalized information column. PostgreSQL can
render equivalent varchar-to-text array casts in two ways after pg_upgrade.
The checker handles those verified differences and retains literal, type, and
duplicate-entry checks. The shipped TimescaleDB upgrade SQL confirms the removed
objects. Permission comparison expands implicit grants and compares effective
rights, because raw grant ordering changed while the 759 relation grants did not.

Initial checks failed on the removed catalog fields and the equivalent array
casts. Acceptance refused the latter until comparison was corrected. The first
tool image lacked a pg_resetwal wrapper; the corrected image passed pg_upgrade.
Test-launcher retries corrected a disposable database name and the Vitest path.
The temporary fingerprint helper also needed its role and grant queries corrected
before the complete comparisons succeeded. Those failed attempts are not counted
as passing checks.

The verified physical backup is well_engine_backup_20261016. The older
well_schema_physical_backup_20261013 remains available.

### Final local conversion

The running stack was converted on 2026-10-16 after the rehearsal passed.
Application writers stopped before the original database shut down. A new cold
copy preserved 10,228,395 application rows and 475,294 aggregate rows. All their
content fingerprints, 15 sequence positions, role records, and effective
relation grants matched after conversion. The target schema passed comparison
with the fresh reference. Engine acceptance added one audit event.

PostgreSQL statistics were rebuilt using vacuumdb --analyze-in-stages. The
extension update also reported its new internal job-history limit of 1,000
successes and failures. Application retention and compression policies matched
the generated reference.

The active volume is well-simulation_timescaledb_pg16_20261016, selected in the
local .env through TIMESCALE_DATA_VOLUME. The original volume,
well-simulation_timescaledb_data, remains PostgreSQL 14 and was verified cleanly
shut down. Both physical backups remain. Returning to the original now would
omit records written after the switch, so it is a recovery source rather than
an automatic rollback destination.

Final checks confirmed:

- Normal installer startup exited successfully on the converted database.
- All six long-running services were healthy. Twin-core reported 12 running
  wells at model version 0.3.0; ingestion reported 216 mappings.
- The API and dashboard returned HTTP 200. The latest historian reading was
  about six seconds old during verification.
- Fresh API and twin database connections retained their restricted privileges.
  Both still lacked schema creation and temporary-table permission.
- Accounts, active controls, and the legacy schema ledger matched their
  pre-shutdown content fingerprints. Exactly one engine acceptance event existed.
- All four continuous-aggregate refresh jobs reported Success with zero failures.
- Background workers and autovacuum were enabled again on the normal server.

Temporary servers, disposable copies, and test credentials were removed after
verification. Protected logical archives remain under /tmp, alongside the
verification records. The database archive alone is insufficient for complete
recovery; retain its roles archive and database-grant instructions as well.

### Delivery review and operational checks, 2026-10-16

Review covered the guarded engine acceptance, transaction rollback on audit
failure, SQL comparison boundaries, pinned images, and preserved-volume selection.
No blocking engine or schema finding remained. The live schema identity and catalog
check passed again without changing grants or application data.

The delivery rerun passed 237 Python tests in the sandbox. Twelve HTTP cases
could not open loopback sockets there; all 12 passed with socket access.
The 51 database cases were opt-in skips in that first run. A separate schema
suite passed 69 cases, including all 38 schema database cases. The 13 model
persistence cases and 89 API cases passed during the upgrade verification above;
they were not repeated for this documentation and delivery review.
Lint, formatting, strict types, OpenAPI validation, and prose checks passed.

Two live telemetry checks also passed:

- Stopping only the gateway made ingestion return 503 and increment its
  reconnect counter. No historian writes occurred after the gateway stopped.
  Restarting the gateway restored fresh writes without restarting ingestion.
  The model remained ready throughout. The first witness compared against a
  time before graceful shutdown and wrongly rejected legitimate final rows.
  A corrected repeat compared against the fully stopped gateway and passed.
- A separate read-only client exercised the deployed poll function over real
  Modbus connections. One deliberately delayed response spanned a gateway
  update. The function retried once, returning all 216 readings with one
  timestamp and version within its normal deadline. It inserted no rows.
  The probe's retry counter is separate from the running ingestion counter.

The owner deferred physical host sleep/wake on 2026-10-16. The passive observer
was stopped without a suspend event. Clock unit tests do not establish a real
suspend result. Both local roadmaps retain this distinction.

Browser fault injection reached the shipped root error page. The initial reload
checks failed because the proxy retained the API's old address and returned 502.
Disabling the test browser's cache did not fix this. Proxy error logs and the
current container addresses established the cause. Reloading nginx restored the
route. Automatic recovery after address changes remained open at that delivery.
The later ADR 0017 amendment records the dynamic proxy and readiness fix.
After the reload, /api/health returned HTTP 200 and the browser error-page
reload returned to login successfully. The injected component error and the
unauthenticated 401 responses were expected parts of that browser check.
An ordinary demo sign-in then rendered the overview, analytics, Alarm Center,
and wellhead detail pages with no browser errors.

## References

- [TimescaleDB issue 8893](https://github.com/timescale/timescaledb/issues/8893):
  the compressed-null defect and the maintainer's binary-upgrade recommendation.
- [PostgreSQL upgrade guide](https://www.tigerdata.com/docs/deploy/self-hosted/upgrades/upgrade-pg):
  the supported engine combinations and pg_upgrade sequence.
- [TimescaleDB Docker upgrade guide](https://www.tigerdata.com/docs/deploy/self-hosted/upgrades/upgrade-docker):
  explicit extension updates in each database.
- [TimescaleDB 2.30.2 release](https://github.com/timescale/timescaledb/releases/tag/2.30.2):
  the target extension release.
- [Schema consolidation](consolidated-schema.md): the original recovery finding.
