# ADR 0008: Use A Containerized Service Baseline

- Status: Accepted
- Date: 2026-05-31

## Context

The project already uses Docker Compose, Python services, PostgreSQL/TimescaleDB, a TypeScript API, and a React dashboard. The current Compose configuration needs repair, but the containerized direction is sound.

## Options Considered

### Option A: Run everything as local scripts

- Pros: Simple for early experiments.
- Cons: Poor reproducibility; weak production signal.

### Option B: Use Docker Compose as the local production-like baseline

- Pros: Reproducible; easy to review; supports multi-service architecture.
- Cons: Not a full production orchestrator.

### Option C: Move directly to Kubernetes

- Pros: Production-oriented deployment model.
- Cons: Premature complexity for this project stage.

## Decision

Use **Docker Compose as the local production-like baseline**.

## Rationale

Compose is the right level for proving service boundaries, networking, health checks, environment configuration, and local reproducibility. Kubernetes can come later after the architecture stabilizes.

## Consequences

- Compose must include database, simulator/twin core, Modbus gateway, ingestion, API, and dashboard.
- Health checks should replace fixed sleeps.
- Internal service ports must be distinct from host-published ports.
- Production deployment docs can later map these service boundaries to Kubernetes or managed services.

