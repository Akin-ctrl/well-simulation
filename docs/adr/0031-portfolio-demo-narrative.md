# ADR 0031: Optimize The Demo For Fast Reviewer Understanding

- Status: Accepted
- Date: 2026-05-31

## Context

This repository is a portfolio project. Reviewers should quickly understand what the system does, why it is relevant to industrial software, and how to run or inspect it.

The project should communicate industrial software range: process simulation, industrial protocol ingestion, time-series storage, alarm logic, APIs, dashboards, and disciplined architecture.

## Options Considered

### Option A: Let reviewers infer value from the folder structure

- Pros: No extra work.
- Cons: Reviewers will miss the project value.

### Option B: Provide deep technical docs only

- Pros: Strong for detailed review.
- Cons: Too slow for first impression.

### Option C: Provide a clear 2-5 minute reviewer journey

- Pros: Makes the value obvious quickly while preserving depth through docs and ADRs.
- Cons: Requires maintaining README, demo script, and screenshots.

## Decision

Use **Option C: optimize the project for a clear 2-5 minute reviewer journey**.

## Reviewer Journey

The intended reviewer flow is:

1. Read the README and understand the project in under 2 minutes.
2. Run the local stack with Docker Compose.
3. Open the dashboard.
4. See live simulated wellhead telemetry and alarms.
5. Run or inspect a what-if scenario.
6. Inspect API and architecture documentation if interested.

## Primary Demo Story

The primary demo should show:

- live wellhead telemetry
- Modbus-style industrial ingestion
- TimescaleDB historian storage
- alarm generation
- model/twin status
- a scenario such as closing a choke and observing lower flow and higher upstream pressure

## Consequences

- README quality is part of the product.
- Docker Compose must be reliable.
- Dashboard should prioritize meaningful live data over visual polish.
- Scenario capability should be implemented early because it demonstrates digital twin behavior.
- Screenshots, diagrams, and demo scripts should be kept current.

