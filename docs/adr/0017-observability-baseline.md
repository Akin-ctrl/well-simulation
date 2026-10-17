# ADR 0017: Use Structured Logs, Health Checks, And Metrics As The Observability Baseline

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Implemented

## Context

The production-grade system must make service health, telemetry freshness, ingestion status, API behavior, forecasts, scenarios, and controls observable.

## Options Considered

### Option A: Plain print/log statements

- Pros: Simple.
- Cons: Weak debugging and poor production signal.

### Option B: Structured logs plus health checks

- Pros: Strong baseline.
- Cons: Limited performance and pipeline visibility.

### Option C: Structured logs, health/readiness endpoints, and metrics

- Pros: Production-grade baseline without too much complexity.
- Cons: Requires metrics discipline.

### Option D: Full OpenTelemetry tracing from day one

- Pros: Excellent distributed visibility.
- Cons: Premature until service boundaries stabilize.

## Decision

Use **Option C: structured logs, health/readiness endpoints, and service-level metrics**.

## Rationale

This gives enough operational visibility to run and debug the system while avoiding the complexity of full distributed tracing too early.

## Consequences

- Services should log structured events to stdout/stderr.
- API and service processes should expose health/readiness endpoints.
- Metrics should cover model ticks, Modbus reads, ingestion writes, API errors, forecasts, scenarios, and controls.
- OpenTelemetry tracing is deferred.

## Amendment 2026-10-16: Check the dashboard's API path

### Context and decision

After the database upgrade, the API had a new container address. The dashboard
still served its page, but nginx sent API requests to the old address.
Every individual service reported healthy while users received 502 responses.

The dashboard image now checks its static page and /api/ready through nginx.
The API readiness check includes its database connection. Docker reports the
dashboard unhealthy when either check fails. Do not reintroduce a static-page-only
health check in Compose; the image owns this command.

Nginx refreshes the API service address through Docker's DNS resolver. Its
shared upstream uses a five-second DNS validity and a two-second lookup timeout.
An API connection attempt times out after three seconds. Proxy request retries
are disabled, so a lost command response cannot trigger a second command attempt.
The image pins the verified nginx 1.27.5 digest, which supports dynamic upstreams.

Manual reloads were a useful recovery step. Fixed API addresses would also avoid
the observed failure, but would couple deployment to address allocation. Dynamic
resolution follows Docker's service-name contract without that coupling.

### Consequences and verification

**Good.** API replacement recovers without a dashboard restart. Readiness tests
the path a browser uses, including the API's database dependency.

**Costs.** This image expects Docker's internal DNS at 127.0.0.11. Another runtime
needs its own trusted resolver. The health check adds two local HTTP requests
every fifteen seconds and becomes unhealthy during API maintenance.

**Accepted risks.** Requests can fail while DNS refreshes or the API is offline.
The client must display that failure and check command status before resubmitting.
Readiness does not establish sensor freshness; telemetry carries its own timestamps.

The isolated regression command is python3 scripts/verify_proxy_recovery.py after
building the service images. It uses real Docker DNS, a real API address change,
the shipped nginx configuration, and the image's health check. A small HTTP fixture
supplies deterministic responses without connecting to the application database.
CI runs the same check after building images.

Verified locally on 2026-10-16: startup without an API, late API startup,
request forwarding, a dropped command response without replay, readiness failure,
and recovery after a real API address change all passed. The last check recovered
in 6.8 seconds without restarting nginx, using a two-second test health interval.
That duration is test evidence, not a production recovery guarantee.

The first regression attempt encountered a connection reset while nginx started.
The bounded startup wait now accepts that temporary connection failure, and the
rerun passed. After deployment, all six services reported healthy, /api/ready
returned 200 through nginx, and the overview, analytics, alarms, and wellhead pages
passed a browser smoke check without browser errors. Remote CI has not run yet.

References: [nginx upstream resolution](https://nginx.org/en/docs/http/ngx_http_upstream_module.html#server),
[Docker DNS](https://docs.docker.com/engine/network/#dns-services), and
[container replacement](https://docs.docker.com/compose/how-tos/networking/#updating-containers-on-the-network).

