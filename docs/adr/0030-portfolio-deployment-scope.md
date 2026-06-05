# ADR 0030: Target A Self-Contained Portfolio Deployment

- Status: Accepted
- Date: 2026-05-31

## Context

This project is a portfolio project intended to demonstrate industrial software thinking, wellhead process simulation, SCADA-style telemetry, and a path toward a lightweight digital twin.

It is not intended to become a scaled production platform. The deployment target should make the project easy for reviewers to understand and run locally, while still showing production-grade engineering practices where they matter.

## Options Considered

### Option A: Design for Kubernetes and cloud deployment

- Pros: Demonstrates enterprise infrastructure awareness.
- Cons: Overengineered for a portfolio project; distracts from the digital twin and simulation work.

### Option B: Keep everything as ad hoc local scripts

- Pros: Simple.
- Cons: Weak reproducibility and poor reviewer experience.

### Option C: Use a self-contained Docker Compose deployment

- Pros: Easy to run locally, demonstrates service boundaries, and supports production-grade practices without scale theater.
- Cons: Not designed for high availability or large-scale deployment.

## Decision

Use **Option C: a self-contained Docker Compose deployment as the primary target**.

## Rationale

The project should show strong engineering judgment, not unnecessary infrastructure. A reviewer should be able to clone the repository, configure environment variables, run Docker Compose, and inspect the full system locally.

## In Scope

- Docker Compose local/demo stack
- clear service boundaries
- health checks and readiness checks
- `.env.example`
- SQL migrations
- structured logs
- metrics endpoints
- OpenAPI documentation
- tests and CI-friendly commands
- architecture diagrams and ADRs

## Out Of Scope

- Kubernetes
- autoscaling
- multi-node deployment
- multi-region deployment
- managed cloud database setup
- enterprise secrets management
- high-availability/failover architecture
- mandatory distributed tracing

## Consequences

- Documentation should optimize for local review and demo execution.
- Production-grade means clarity, reliability, security hygiene, and testability, not large-scale infrastructure.
- Future deployment docs should not imply cloud-scale requirements.

