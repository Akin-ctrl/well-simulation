import {
  pgTable,
  unique,
  serial,
  varchar,
  text,
  foreignKey,
  integer,
  doublePrecision,
  smallint,
  boolean,
  timestamp,
  index,
  primaryKey,
  bigserial,
  bigint,
  jsonb,
  uuid,
} from 'drizzle-orm/pg-core';
import { users } from './user';

export const field = pgTable(
  'field',
  {
    fieldId: serial('field_id').primaryKey().notNull(),
    name: varchar({ length: 255 }).notNull(),
    description: text(),
  },
  (table) => [unique('field_name_key').on(table.name)]
);

export const location = pgTable(
  'location',
  {
    locationId: serial('location_id').primaryKey().notNull(),
    fieldId: integer('field_id').notNull(),
    name: varchar({ length: 255 }).notNull(),
    address: text(),
    latitude: doublePrecision(),
    longitude: doublePrecision(),
  },
  (table) => [
    foreignKey({
      columns: [table.fieldId],
      foreignColumns: [field.fieldId],
      name: 'location_field_id_fkey',
    }).onDelete('cascade'),
  ]
);

export const wellhead = pgTable(
  'wellhead',
  {
    wellheadId: serial('wellhead_id').primaryKey().notNull(),
    locationId: integer('location_id').notNull(),
    deviceId: integer('device_id').notNull(),
    name: varchar({ length: 255 }).notNull(),
    type: varchar({ length: 100 }),
    status: varchar({ length: 50 }).default('active'),
  },
  (table) => [
    foreignKey({
      columns: [table.deviceId],
      foreignColumns: [device.deviceId],
      name: 'wellhead_device_id_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.locationId],
      foreignColumns: [location.locationId],
      name: 'wellhead_location_id_fkey',
    }).onDelete('cascade'),
    unique('wellhead_device_id_key').on(table.deviceId),
    unique('wellhead_name_key').on(table.name),
  ]
);

export const device = pgTable(
  'device',
  {
    deviceId: serial('device_id').primaryKey().notNull(),
    name: varchar({ length: 255 }).notNull(),
    modbusUnitId: integer('modbus_unit_id').notNull(),
    status: varchar({ length: 50 }).default('active'),
  },
  (table) => [unique('device_name_key').on(table.name)]
);

export const deviceparametermapping = pgTable(
  'deviceparametermapping',
  {
    mappingId: serial('mapping_id').primaryKey().notNull(),
    deviceId: integer('device_id').notNull(),
    parameterTypeId: integer('parameter_type_id').notNull(),
    modbusRegister: integer('modbus_register').notNull(),
    functionCode: smallint('function_code').notNull(),
    registerType: varchar('register_type', { length: 50 }).notNull(),
    active: boolean().default(true),
  },
  (table) => [
    foreignKey({
      columns: [table.deviceId],
      foreignColumns: [device.deviceId],
      name: 'deviceparametermapping_device_id_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.parameterTypeId],
      foreignColumns: [parametertype.parameterTypeId],
      name: 'deviceparametermapping_parameter_type_id_fkey',
    }).onDelete('cascade'),
    unique('deviceparametermapping_device_id_modbus_register_key').on(
      table.deviceId,
      table.modbusRegister
    ),
  ]
);

export const parametertype = pgTable(
  'parametertype',
  {
    parameterTypeId: serial('parameter_type_id').primaryKey().notNull(),
    code: varchar({ length: 100 }).notNull(),
    displayName: varchar('display_name', { length: 255 }).notNull(),
    canonicalUnit: varchar('canonical_unit', { length: 50 }),
    dataType: varchar('data_type', { length: 50 }).notNull(),
    normalMin: doublePrecision('normal_min'),
    normalMax: doublePrecision('normal_max'),
  },
  (table) => [unique('parametertype_code_key').on(table.code)]
);

export const alarmrule = pgTable(
  'alarmrule',
  {
    alarmRuleId: serial('alarm_rule_id').primaryKey().notNull(),
    parameterTypeId: integer('parameter_type_id').notNull(),
    severityLevel: varchar('severity_level', { length: 50 }).notNull(),
    operator: varchar({ length: 10 }).notNull(),
    thresholdValue: doublePrecision('threshold_value').notNull(),
    active: boolean().default(true),
    createdAt: timestamp('created_at', {
      withTimezone: true,
      mode: 'string',
    }).defaultNow(),
  },
  (table) => [
    foreignKey({
      columns: [table.parameterTypeId],
      foreignColumns: [parametertype.parameterTypeId],
      name: 'alarmrule_parameter_type_id_fkey',
    }).onDelete('cascade'),
  ]
);

export const parameterreading = pgTable(
  'parameterreading',
  {
    parameterReadingId: bigserial('parameter_reading_id', {
      mode: 'bigint',
    }).notNull(),
    timestampUtc: timestamp('timestamp_utc', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    wellheadId: integer('wellhead_id').notNull(),
    parameterTypeId: integer('parameter_type_id').notNull(),
    mappingId: integer('mapping_id').notNull(),
    rawValue: doublePrecision('raw_value').notNull(),
    insertedAt: timestamp('inserted_at', {
      withTimezone: true,
      mode: 'string',
    }).defaultNow(),
    // Migration 0007: which generator produced the reading. The check
    // constraint on the allowed values lives in the SQL migration.
    sourceKind: text('source_kind').notNull().default('synthetic_random_simulator'),
    // Migration 0008: the model version, such as '0.3.0'. NULL means not
    // recorded. The column's real type is the domain model_version_text, which
    // checks the format; Drizzle sees it as text.
    modelVersion: text('model_version'),
  },
  (table) => [
    index('parameterreading_timestamp_utc_idx').using(
      'btree',
      table.timestampUtc.desc().nullsFirst().op('timestamptz_ops')
    ),
    foreignKey({
      columns: [table.mappingId],
      foreignColumns: [deviceparametermapping.mappingId],
      name: 'parameterreading_mapping_id_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.parameterTypeId],
      foreignColumns: [parametertype.parameterTypeId],
      name: 'parameterreading_parameter_type_id_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.wellheadId],
      foreignColumns: [wellhead.wellheadId],
      name: 'parameterreading_wellhead_id_fkey',
    }).onDelete('cascade'),
    primaryKey({
      columns: [table.parameterReadingId, table.timestampUtc],
      name: 'parameterreading_pkey',
    }),
  ]
);

// Migration 0010 owns the Timescale hypertables and the model_version_text
// domain. Drizzle mirrors their columns for typed application reads.
export const twinStateSnapshot = pgTable(
  'twin_state_snapshot',
  {
    snapshotId: bigserial('snapshot_id', { mode: 'bigint' }).notNull(),
    simulatedAt: timestamp('simulated_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId),
    modelVersion: text('model_version').notNull(),
    parameterFingerprint: text('parameter_fingerprint').notNull(),
    status: text('status').notNull(),
    tickIndex: bigint('tick_index', { mode: 'bigint' }).notNull(),
    skippedTicks: bigint('skipped_ticks', { mode: 'bigint' }).notNull(),
    tubingPressurePsi: doublePrecision('tubing_pressure_psi').notNull(),
    casingPressurePsi: doublePrecision('casing_pressure_psi').notNull(),
    flowRateBpd: doublePrecision('flow_rate_bpd').notNull(),
    wellheadTemperatureF: doublePrecision('wellhead_temperature_f').notNull(),
    waterCutPercent: doublePrecision('water_cut_percent').notNull(),
    chokePositionPercent: doublePrecision('choke_position_percent').notNull(),
    blockageFactor: doublePrecision('blockage_factor').notNull(),
    corrosionFactor: doublePrecision('corrosion_factor').notNull(),
    state: jsonb('state').$type<Record<string, unknown>>().notNull(),
    insertedAt: timestamp('inserted_at', { withTimezone: true, mode: 'string' })
      .defaultNow()
      .notNull(),
  },
  (table) => [
    primaryKey({
      columns: [table.snapshotId, table.simulatedAt],
      name: 'twin_state_snapshot_pkey',
    }),
    index('ix_twin_state_snapshot_well_time').on(
      table.wellheadId,
      table.simulatedAt.desc()
    ),
  ]
);

export const twinRecoveryEvent = pgTable(
  'twin_recovery_event',
  {
    eventId: bigserial('event_id', { mode: 'bigint' }).notNull(),
    occurredAt: timestamp('occurred_at', { withTimezone: true, mode: 'string' })
      .defaultNow()
      .notNull(),
    wellheadId: integer('wellhead_id').references(() => wellhead.wellheadId),
    eventType: text('event_type').notNull(),
    modelVersion: text('model_version').notNull(),
    snapshotSimulatedAt: timestamp('snapshot_simulated_at', {
      withTimezone: true,
      mode: 'string',
    }),
    gapSeconds: doublePrecision('gap_seconds'),
    details: jsonb('details').$type<Record<string, unknown>>().default({}).notNull(),
  },
  (table) => [
    primaryKey({
      columns: [table.eventId, table.occurredAt],
      name: 'twin_recovery_event_pkey',
    }),
    index('ix_twin_recovery_event_well_time').on(
      table.wellheadId,
      table.occurredAt.desc()
    ),
  ]
);

export const wellheadModelParameter = pgTable(
  'wellhead_model_parameter',
  {
    parameterOverrideId: serial('parameter_override_id').primaryKey().notNull(),
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId),
    modelVersion: text('model_version').notNull(),
    parameterKey: text('parameter_key').notNull(),
    value: doublePrecision('value'),
    unit: text('unit'),
    source: text('source').notNull(),
    changedBy: text('changed_by').notNull(),
    reason: text('reason').notNull(),
    validFrom: timestamp('valid_from', { withTimezone: true, mode: 'string' })
      .defaultNow()
      .notNull(),
  },
  (table) => [
    unique(
      'wellhead_model_parameter_wellhead_id_model_version_parameter_key_valid_from_key'
    ).on(table.wellheadId, table.modelVersion, table.parameterKey, table.validFrom),
    index('ix_wellhead_model_parameter_current').on(
      table.wellheadId,
      table.modelVersion,
      table.parameterKey,
      table.validFrom.desc()
    ),
  ]
);

// Migration 0011 keeps run metadata in ordinary tables and partitions future
// points by time. The time column is part of each hypertable primary key.
export const predictionRun = pgTable(
  'prediction_run',
  {
    runId: uuid('run_id').primaryKey().notNull(),
    createdAt: timestamp('created_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId),
    requestedByUserId: integer('requested_by_user_id')
      .notNull()
      .references(() => users.id),
    modelVersion: text('model_version').notNull(),
    parameterFingerprint: text('parameter_fingerprint').notNull(),
    baseSimulatedAt: timestamp('base_simulated_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    horizonSeconds: integer('horizon_seconds').notNull(),
    stepSeconds: integer('step_seconds').notNull(),
    baseState: jsonb('base_state').$type<Record<string, unknown>>().notNull(),
    parameterValues: jsonb('parameter_values')
      .$type<Record<string, number>>()
      .notNull(),
    controlValues: jsonb('control_values')
      .$type<Record<string, number | boolean>>()
      .notNull(),
    alarmRules: jsonb('alarm_rules').$type<Record<string, unknown>[]>().notNull(),
    confidenceScore: doublePrecision('confidence_score').notNull(),
    confidenceReasons: jsonb('confidence_reasons').$type<string[]>().notNull(),
  },
  (table) => [
    index('ix_prediction_run_well_created').on(
      table.wellheadId,
      table.createdAt.desc()
    ),
  ]
);

export const predictionPoint = pgTable(
  'prediction_point',
  {
    runId: uuid('run_id')
      .notNull()
      .references(() => predictionRun.runId),
    forecastAt: timestamp('forecast_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    elapsedSeconds: integer('elapsed_seconds').notNull(),
    values: jsonb('values_json').$type<Record<string, number>>().notNull(),
    tubingPressurePsi: doublePrecision('tubing_pressure_psi').notNull(),
    flowRateBpd: doublePrecision('flow_rate_bpd').notNull(),
    wellheadTemperatureF: doublePrecision('wellhead_temperature_f').notNull(),
    waterCutPercent: doublePrecision('water_cut_percent').notNull(),
    alarmRisk: text('alarm_risk').notNull(),
    triggeredRuleIds: jsonb('triggered_rule_ids').$type<number[]>().notNull(),
  },
  (table) => [
    primaryKey({
      columns: [table.runId, table.forecastAt],
      name: 'prediction_point_pkey',
    }),
  ]
);

export const scenarioRun = pgTable(
  'scenario_run',
  {
    runId: uuid('run_id').primaryKey().notNull(),
    createdAt: timestamp('created_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId),
    requestedByUserId: integer('requested_by_user_id')
      .notNull()
      .references(() => users.id),
    name: varchar('name', { length: 120 }).notNull(),
    modelVersion: text('model_version').notNull(),
    parameterFingerprint: text('parameter_fingerprint').notNull(),
    baseSimulatedAt: timestamp('base_simulated_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    horizonSeconds: integer('horizon_seconds').notNull(),
    stepSeconds: integer('step_seconds').notNull(),
    baseState: jsonb('base_state').$type<Record<string, unknown>>().notNull(),
    parameterValues: jsonb('parameter_values')
      .$type<Record<string, number>>()
      .notNull(),
    controlValues: jsonb('control_values')
      .$type<Record<string, number | boolean>>()
      .notNull(),
    scheduledControls: jsonb('scheduled_controls')
      .$type<Record<string, unknown>[]>()
      .notNull(),
    alarmRules: jsonb('alarm_rules').$type<Record<string, unknown>[]>().notNull(),
    confidenceScore: doublePrecision('confidence_score').notNull(),
    confidenceReasons: jsonb('confidence_reasons').$type<string[]>().notNull(),
  },
  (table) => [
    index('ix_scenario_run_well_created').on(table.wellheadId, table.createdAt.desc()),
  ]
);

export const scenarioPoint = pgTable(
  'scenario_point',
  {
    runId: uuid('run_id')
      .notNull()
      .references(() => scenarioRun.runId),
    forecastAt: timestamp('forecast_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    elapsedSeconds: integer('elapsed_seconds').notNull(),
    values: jsonb('values_json').$type<Record<string, number>>().notNull(),
    tubingPressurePsi: doublePrecision('tubing_pressure_psi').notNull(),
    flowRateBpd: doublePrecision('flow_rate_bpd').notNull(),
    wellheadTemperatureF: doublePrecision('wellhead_temperature_f').notNull(),
    waterCutPercent: doublePrecision('water_cut_percent').notNull(),
    baselineValues: jsonb('baseline_values_json')
      .$type<Record<string, number>>()
      .notNull(),
    alarmRisk: text('alarm_risk').notNull(),
    baselineAlarmRisk: text('baseline_alarm_risk').notNull(),
    triggeredRuleIds: jsonb('triggered_rule_ids').$type<number[]>().notNull(),
  },
  (table) => [
    primaryKey({
      columns: [table.runId, table.forecastAt],
      name: 'scenario_point_pkey',
    }),
  ]
);

export const alarmevent = pgTable(
  'alarmevent',
  {
    eventId: bigserial('event_id', { mode: 'bigint' }).notNull(),
    alarmRuleId: integer('alarm_rule_id').notNull(),
    // You can use { mode: "bigint" } if numbers are exceeding js number limitations
    parameterReadingId: bigint('parameter_reading_id', {
      mode: 'number',
    }).notNull(),
    timestampUtc: timestamp('timestamp_utc', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    wellheadId: integer('wellhead_id').notNull(),
    triggeredAt: timestamp('triggered_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    clearedAt: timestamp('cleared_at', { withTimezone: true, mode: 'string' }),
    severityLevel: varchar('severity_level', { length: 50 }).notNull(),
    triggeredValue: doublePrecision('triggered_value').notNull(),
  },
  (table) => [
    index('alarmevent_triggered_at_idx').using(
      'btree',
      table.triggeredAt.desc().nullsFirst().op('timestamptz_ops')
    ),
    foreignKey({
      columns: [table.alarmRuleId],
      foreignColumns: [alarmrule.alarmRuleId],
      name: 'alarmevent_alarm_rule_id_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.wellheadId],
      foreignColumns: [wellhead.wellheadId],
      name: 'alarmevent_wellhead_id_fkey',
    }).onDelete('cascade'),
    primaryKey({
      columns: [table.eventId, table.triggeredAt],
      name: 'alarmevent_pkey',
    }),
  ]
);
