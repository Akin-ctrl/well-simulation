# ADR 0004: Keep Modbus TCP As The Industrial Edge Interface

- Status: Accepted
- Date: 2026-05-31

## Context

The current project uses Modbus TCP between the simulator/gateway and ingestion service. A future twin still needs an industrial-facing interface to preserve SCADA relevance.

## Options Considered

### Option A: Remove Modbus and write directly to the database

- Pros: Simpler architecture; fewer moving parts.
- Cons: Loses the industrial protocol signal and edge-ingestion realism.

### Option B: Keep Modbus TCP

- Pros: Simple, recognizable industrial protocol; already implemented.
- Cons: Limited semantic richness compared with modern IoT protocols.

### Option C: Replace Modbus with MQTT/OPC UA immediately

- Pros: More modern and expressive.
- Cons: Larger migration; distracts from the digital twin core.

## Decision

Keep **Modbus TCP** as the initial industrial edge interface.

## Rationale

Modbus is enough to demonstrate industrial protocol ingestion and keeps continuity with the existing project. MQTT or OPC UA can be added later through adapters.

## Consequences

- The twin core should not depend directly on Modbus.
- A telemetry adapter should map model state to Modbus registers.
- Future protocol support should be additive, not a rewrite.

