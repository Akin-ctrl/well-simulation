# Milestone 6: Model Telemetry Through Modbus

## Purpose

A well's pressure and flow now come from the running process model. The Modbus
server still presents holding registers, ingestion still polls them with Modbus
TCP, and TimescaleDB still stores the decoded readings. This preserves the
industrial read path while replacing the random telemetry source.

The values remain synthetic and uncalibrated. There is no physical device or
actuator behind this connection. The dashboard is unchanged in this milestone.

## System design

```text
one-second model ticks -> GET /model-snapshot -> gateway validation
  -> mapped holding registers -> Modbus TCP polling -> ingestion
  -> parameterReading with source time and source kind -> historian reads
```

Twin-core returns all active well states in one registry snapshot. Its endpoint
returns HTTP 503 until the fleet and tick loop are ready. The gateway requests
that snapshot every `TELEMETRY_INTERVAL_SECONDS` (five seconds by default), with
a three-second network timeout and a one-megabyte response limit. It checks the
model source, running status, complete mapped fleet, common UTC model tick,
finite field values, and age. It then converts supported model fields to the
existing parameter codes and writes their mapped Modbus registers.

The gateway owns the field-to-parameter conversion in `model_source.py`.
Twin-core does not need to know register addresses or Modbus types. The gateway
and ingestion use the same supported-mapping function, so a newly added
parameter without a model equation cannot silently appear as zero. Metadata
still supplies register offsets, unit IDs, parameter types, and well IDs.

## Supported values and units

| Parameter code | Model field | Unit and encoding |
| --- | --- | --- |
| `tubing_pressure` | `tubingPressurePsi` | psi, float32 |
| `casing_pressure` | `casingPressurePsi` | psi, float32 |
| `annulus_pressure` | `annulusPressurePsi` | psi, float32 |
| `gas_oil_ratio` | `gasOilRatioScfStb` | scf/stb, float32 |
| `sand_detector` | `sandDetectorPpm` | mass ppm, float32 |
| `corrosion_rate` | `corrosionRateMpy` | mils/year, float32 |
| `h2s_level` | `h2sLevelPpm` | gas mole ppm, float32 |
| `co2_level` | `co2LevelPercent` | gas mole percent, float32 |
| `vibration` | `vibrationMmS` | RMS mm/s, float32 |
| `wellhead_temperature` | `wellheadTemperatureF` | degrees Fahrenheit, float32 |
| `choke_valve_position` | `chokePositionPercent` | percent, rounded int32 |
| `flow_rate` | `flowRateBpd` | barrels/day, float32 |
| `water_cut` | `waterCutPercent` | percent, float32 |
| `master_valve_status` | `masterValveOpen` | 0 or 1, int32 |
| `wing_valve_status` | `wingValveOpen` | 0 or 1, int32 |
| `swab_valve_status` | `swabValveOpen` | 0 or 1, int32 |
| `emergency_shutdown` | `emergencyShutdown` | 0 or 1, int32 |
| `pump_status` | `pumpOn` | 0 or 1, int32 |

The seven later-added signals come from the same version 0.3.0 synthetic
reference well. [The physics design](reference-well-physics.md) defines their
units, equations, assumptions, and limits. `corrosionFactor` remains a separate
dimensionless index and is not mapped to the corrosion-rate register. Historical
random readings retain their original source label until retention removes them.

## Modbus frame and freshness logic

The gateway remains a Modbus TCP server. Ingestion sends function code 03
(Read Holding Registers) with the database's zero-based register offset and a
quantity of two. The server sends two 16 bit words for each 32 bit value. A
single word has its most significant byte first, as the Modbus specification
requires. The project register map uses the low word first across the two words.
Both encoder and decoder use the shared `WORD_ORDER` constant and a round-trip
test. The TCP header carries a transaction ID, protocol ID, length, and unit
ID. It carries no measurement time or quality flag.

Register 1900 is a two-word unsigned UTC epoch second for the model tick. The
gateway writes zero to the heartbeat before changing any signal register, then
writes the new tick time after all signal writes complete. Ingestion reads that
register on every mapped Modbus unit before and after polling. It stores a batch
only if all units agree on the same nonzero tick in both reads, the tick advances
beyond the last stored tick, and it is no more than twice the telemetry
interval old (minimum five seconds). A value more than two
seconds in the future is rejected. The second read detects a batch changed
mid-poll, including a change on a different unit. The zero marks an update in
progress or an incomplete write.

`timestamp_utc` in `parameterReading` is the model tick time carried by the
heartbeat. `inserted_at` is the database insertion time. The new `source_kind`
column marks old rows as `synthetic_random_simulator` and new rows as
`synthetic_reduced_order_model`. Historian reads include the source on each
reading and report `mixed` when old and new values appear together. No
measurement is presented as observed plant data.

## Failure and recovery behavior

| Condition | Behavior |
| --- | --- |
| Twin-core is offline or times out | Gateway becomes unready; heartbeat does not advance. Ingestion skips later polls. |
| Twin-core returns HTTP 503 | Gateway retries at the next fixed interval and does not publish an old tick. |
| Any mapped well is missing or degraded | The complete batch is rejected. |
| A value is malformed or outside its basic physical range | The batch is rejected before any register write. |
| A register write fails | Heartbeat remains zero; ingestion skips the incomplete image. |
| A Modbus read fails | Ingestion rejects the poll and reconnects; it does not insert a partial batch. |
| Metadata changes | Both services reload it. The gateway clears its register image before using the new map. |
| Database is offline | Ingestion retries with bounded backoff and reports unready. No local store-and-forward exists. |
| Service restarts | The model cold-starts until Milestone 7 adds snapshots. The gateway waits for a new running tick. |

The bounded buffer here is one current register image. It does not queue missed
samples. Missed model ticks are reported through health, readiness, logs, and
metrics. Modbus and the internal HTTP endpoint are confined to the Compose
network. Modbus itself provides no authentication or encryption; this service
is a simulator, not a safe plant-network endpoint.

## Standards and verification

The Modbus function code, zero-based offsets, two-register quantity, TCP
header, and absent wire-level timestamp were checked against the spec-verified
Modbus reference in the industrial project skill. The register codec remains
fixed by exact-word and round-trip tests. Boundary validation rejects malformed
model snapshots and incompatible metadata. The historian migration preserves
older rows and is applied before service startup. No control write endpoint or
physical device command is added.

The alternative of writing model values directly to TimescaleDB would remove
the Modbus protocol demonstration. Fetching each well separately could combine
states from different ticks. One fleet snapshot and the existing Modbus polling
path are therefore used. The extra gateway validation and heartbeat handshake
cost code and network traffic, and the 32 bit conversion loses some numeric
precision. Those costs are accepted for a traceable industrial read path.
