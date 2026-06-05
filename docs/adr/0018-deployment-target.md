# ADR 0018: Use Docker Compose As The First Production-Grade Deployment Target

- Status: Accepted
- Date: 2026-05-31

## Context

The project already uses Docker Compose, but the current deployment needs cleanup. The first production-grade target should support repeatable local operation and clear service boundaries without jumping too early to Kubernetes.

## Options Considered

### Option A: Local scripts

- Pros: Simple.
- Cons: Not production-grade and hard to reproduce.

### Option B: Docker Compose

- Pros: Matches current project shape; good for local review, demos, and small deployments.
- Cons: Not a full production orchestrator.

### Option C: Kubernetes

- Pros: Strong production orchestration story.
- Cons: Premature complexity.

### Option D: Managed PaaS plus managed database

- Pros: Operationally simple for web apps.
- Cons: Awkward for Modbus and multi-service industrial simulation.

## Decision

Use **Option B: Docker Compose as the first production-grade deployment target**.

## Rationale

Compose is the right level for proving service boundaries, networking, health checks, migrations, and reproducible deployment. Kubernetes can be considered after the service model stabilizes.

## Consequences

- Compose should include `db`, `twin-core`, `modbus-gateway`, `ingestion`, `api`, and `dashboard`.
- Health checks and retries should replace fixed startup sleeps.
- Internal service ports must be separated from host-published ports.
- `.env.example` should document required configuration.

