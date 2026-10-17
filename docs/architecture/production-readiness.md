# Production readiness

Status reviewed against application commit `82c9baf`, 2026-10-17.

The local synthetic application has verification evidence for its main workflows.
It is not a field-calibrated system or a deployed production service. This record
separates implemented behavior from open work and the limits of the checks run.

## How to read the status

- **Verified locally:** the linked record describes a completed check against
  the implementation at that time. It does not imply remote CI passed.
- **Implemented, limited:** code exists, with the limitation stated in the row.
- **Open:** the delivery action or capability remains unfinished.
- **Deferred:** the owner explicitly chose to postpone the check.
- **Outside current scope:** it needs a separate design and authorization.

## Local implementation and evidence

| Area | Status | Evidence and limits |
| --- | --- | --- |
| Fresh installation | Verified locally | The [usability delivery](../design/dashboard-usability.md#delivery-evidence-2026-10-17) built and started a clean candidate with fresh storage |
| Current deployment | Verified locally | The [alarm delivery](../design/alarm-lifecycle.md#verification-on-2026-10-17) checked eight healthy services, four alarm views and unchanged controls for 12 wells |
| Browser-to-API recovery | Verified locally | [ADR 0017](../adr/0017-observability-baseline.md) records dependency failure and API address-change recovery |
| Telemetry failure and freshness | Verified locally | [Engine follow-up](../design/database-engine-upgrade.md#delivery-review-and-operational-checks-2026-10-16) records gateway recovery and poll-collision checks; stale data remains visible |
| Model state and restart | Verified locally | [Milestone 7](../design/twin-core-milestone-7.md) covers snapshots, restored state and recorded gaps; physical sleep/wake is separate |
| Controls and parameters | Verified locally | [Operating screens](../design/operating-workspace.md#verification-on-2026-10-17) exercised real command application, restoration and parameter adoption after restart |
| Current and historical analyses | Verified locally | [Milestone 8](../design/twin-core-milestone-8.md) and [historical scenarios](../design/historical-scenarios.md) cover private model copies and saved results |
| Scheduled forecasts | Verified locally | [Schedule verification](../design/scheduled-forecasts.md#implementation-verification-on-2026-10-17) covers authority, competing claims, expiry and atomic result saving |
| Alarm lifecycle | Verified locally | [Alarm verification](../design/alarm-lifecycle.md#verification-on-2026-10-17) covers exact identities, permissions, competing writes, expiry and lost replies |
| Schema and existing data | Verified locally | [Consolidation](../design/consolidated-schema.md), [engine conversion](../design/database-engine-upgrade.md) and [extensions](../design/schema-extension.md) record separate preservation checks |
| Backup restoration | Verified locally | The final alarm rehearsal preserved 3,657,848 records across 26 existing tables; this is a rehearsal, not an automated backup service |
| Retention and compression | Implemented, limited | [ADR 0026](../adr/0026-retention-and-compression.md#amendment-2026-10-17-record-the-installed-retention-policy) lists actual policies and ordinary tables without automatic expiry |
| Health, readiness and metrics | Verified locally | [Observability verification](../design/observability.md#verification-on-2026-10-17) covers four scrape targets, separate readiness and reachability, and failed-source behavior |
| Shared contracts and API behavior | Verified locally | The final alarm record reports 127 API tests and 40 shared-contract/frontend tests; OpenAPI validation checks structure, not every runtime response |
| Accessibility | Implemented, limited | [Browser and Orca evidence](../design/milestone-9/accessibility-verification.md) states the tested combinations; later designs record their own checks, without claiming universal coverage |
| CI configuration | Implemented, limited | [CI](../../.github/workflows/ci.yml) defines types, lint, formatting, Python, schema/API, image, proxy, browser and metrics checks; remote execution for this delivery remains open |

## Security and accounts

The API issues HTTP-only session cookies. The local HTTP setup uses
`COOKIE_SECURE=false`; a deployment that terminates TLS must configure the secure
flag and allowed origins explicitly. Logout increments the account's session
version and invalidates its existing tokens. Operating writes recheck account
authority and record an attributed result. ADMIN and OPERATIONS remain separate.

The API uses a restricted database login. Twin-core has separate grants for
metadata, snapshots, projections and command application. The gateway, ingestion
and schema setup still use the configured database administrator. This is a local deployment
boundary, not a claim that every service has a separate least-privilege account.

Ordinary role guards use the role in the session token. Some reads can therefore
retain an old role until session revocation or expiry. Sensitive operating paths
recheck the database account, and scheduled forecasts check their owner's current
authority. Public account disabling and role-editing routes are not implemented.

Routine logs exclude credentials. The bootstrap administrator password is
deliberately logged once by the installer. The repository also contains a known
demo USER password. Both are documented local conveniences. Private security
reports follow the [security policy](../../.github/SECURITY.md).

## Secrets and network exposure

Runtime configuration reads secrets from environment variables. Compose loads
the ignored `.env`; `.env.example` contains placeholders. The application validates
required database settings, the JWT signing secret and the internal service token.
Grafana separately validates its optional administrator password.

A secret store could populate the same environment names, but its access control,
rotation, deployment and recovery still need implementation and verification.
Changing `JWT_SECRET` currently invalidates all sessions. There is no overlapping
signing-key rotation window.

| Default host port | Service | Boundary |
| --- | --- | --- |
| 8090 | Dashboard and `/api` proxy | Published on host interfaces; plain HTTP in the local setup |
| 5434 | Database | Published on host interfaces; database authentication required |
| 5020 | Modbus gateway | Published on host interfaces; no application login |
| 9090, optional | Prometheus | Host loopback only; no login |
| 3010, optional | Grafana | Host loopback only; separate administrator account |

The API, twin-core and telemetry health ports otherwise stay inside the Compose
network. The host-development recipe explicitly adds a loopback twin-core port.
The dashboard proxy exposes `/api/metrics` without login, alongside the API's
health probes. It reports service activity rather than well readings.
Remote exposure needs its own network, authentication and TLS design.

## Open work and deployment limits

| Item | Status | What closes it |
| --- | --- | --- |
| Remote CI and publication | Open | Publish the approved branch and inspect the actual remote results |
| Contributor licence consent | Open | Record the required contributor consent and complete the licence decision |
| Physical host sleep/wake | Deferred by owner | A coordinated physical sleep/wake check with recorded recovery observations |
| Full assistive-technology coverage | Open | Additional human testing across the intended browser and assistive-technology combinations |
| Routine backup operation | Outside current scope | A scheduled backup, retention, monitoring and restore-drill design |
| Permanent command, job and alarm-action archives | Outside current scope | An archive policy that preserves receipt references and audit meaning |
| Field accuracy | Outside current scope | Measured well data, calibration and independent validation for a defined use |
| Physical equipment control | Outside current scope | A separate device, authority and safety design |
| General user/role, alarm-rule and asset editing | Outside current scope | Separate reviewed administration workflows |
| Remote production deployment | Outside current scope | TLS, network boundaries, credential rotation, service grants and operational ownership |
| Multiple model instances and failover | Outside current scope | Explicit ownership and recovery design; current model operation assumes one instance |

These entries do not reopen completed remediation phases. They state the limits
of the local deployment and the remaining delivery actions. The documentation
update reviewed existing evidence; it did not rerun the application test suites,
perform another database conversion or check remote publication state.

## Verification fixes on 2026-10-17

A fresh audit after documentation commit `1e6a802` found three Python type
errors, a source formatting failure, insufficient contrast in the stale telemetry
label, a historical test that needed an existing snapshot, and 14 model database
tests that CI did not enable.

The fixes check database rows before indexing them, format the affected statement,
exclude generated test and type-check caches from Prettier, and darken the stale
label. The browser accessibility test now supplies stale telemetry explicitly.
It no longer needs readings to age during the test to exercise that state.

Control and projection tests now reuse the schema suite's temporary databases.
Each test provisions separate service roles through the application's grant
functions and removes those roles during cleanup. The historical test writes
its own snapshot and verifies that the saved source stays unchanged after a
scenario branches from it. CI enables all these tests with
`SCHEMA_DATABASE_TEST=1`; the [database test instructions](../../data/README.md#changing-the-schema)
give the same command for local use.

| Check | Fresh result |
| --- | --- |
| Python, including database suites | 383 passed, zero skipped |
| Model database checks within that total | All 14 control and projection tests passed; no test databases or service roles remained afterward |
| API integration | 127 passed |
| Shared-contract and frontend unit tests | 40 passed |
| Chromium browser workflows | All nine passed against the updated isolated dashboard, including stale telemetry accessibility |
| Types, lint and formatting | Python and TypeScript checks passed; Prettier passed with generated caches and browser artifacts present |
| Dashboard image | Built successfully; isolated dashboard became healthy |
| Documentation | Prose, local links and OpenAPI validation passed |

The first disposable database startup exceeded its 60-second deadline while the
build and TypeScript checks ran. No tests ran in that attempt. After those tasks
finished, a retry with a longer startup deadline completed all database suites.
Both temporary database containers were removed. Host JavaScript checks used
Node 24; the dashboard image uses the repository's Node 22 build.

These are local results. This follow-up did not run GitHub Actions, repeat backup
restoration, or perform the deferred physical sleep/wake test. The field accuracy
and manual assistive-technology limits above still apply.
