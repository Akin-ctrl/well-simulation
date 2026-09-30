import {
  varchar,
  integer,
  doublePrecision,
  timestamp,
  bigint,
  pgView,
} from 'drizzle-orm/pg-core';

/**
 * Read-only mirrors of views and continuous aggregates owned by the SQL
 * migrations in `data/sql/migrations`, per ADR 0006.
 *
 * These are declared with `.existing()` deliberately. The previous definitions
 * carried a `.as()` body introspected from the database, which pointed at
 * `_timescaledb_internal._materialized_hypertable_N`. Those internal names are
 * assigned in creation order and are not stable across a rebuild, so any
 * `drizzle-kit push` would have emitted DDL against TimescaleDB internals.
 * Declaring them as existing gives typed reads without ever generating DDL.
 */

export const vWellheadParameterReadings = pgView('v_wellhead_parameter_readings', {
  timestampUtc: timestamp('timestamp_utc', {
    withTimezone: true,
    mode: 'string',
  }),
  rawValue: doublePrecision('raw_value'),
  wellheadId: integer('wellhead_id'),
  wellheadName: varchar('wellhead_name', { length: 255 }),
  wellheadType: varchar('wellhead_type', { length: 100 }),
  locationId: integer('location_id'),
  locationName: varchar('location_name', { length: 255 }),
  fieldId: integer('field_id'),
  fieldName: varchar('field_name', { length: 255 }),
  parameterTypeId: integer('parameter_type_id'),
  parameterCode: varchar('parameter_code', { length: 100 }),
  parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
  canonicalUnit: varchar('canonical_unit', { length: 50 }),
  dataType: varchar('data_type', { length: 50 }),
  normalMin: doublePrecision('normal_min'),
  normalMax: doublePrecision('normal_max'),
}).existing();

/**
 * Trend columns shared by the three parameter-group aggregates.
 *
 * Named for their real bucket width. They were previously called
 * `mv_hourly_*` while bucketing at two minutes.
 */
const trendColumns = {
  bucketTime: timestamp('bucket_time', { withTimezone: true, mode: 'string' }),
  wellheadId: integer('wellhead_id'),
  wellheadName: varchar('wellhead_name', { length: 255 }),
  locationName: varchar('location_name', { length: 255 }),
  fieldName: varchar('field_name', { length: 255 }),
  parameterCode: varchar('parameter_code', { length: 100 }),
  parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
  canonicalUnit: varchar('canonical_unit', { length: 50 }),
  avgValue: doublePrecision('avg_value'),
  minValue: doublePrecision('min_value'),
  maxValue: doublePrecision('max_value'),
  readingCount: bigint('reading_count', { mode: 'number' }),
};

export const mv2minPressureTrends = pgView(
  'mv_2min_pressure_trends',
  trendColumns
).existing();

export const mv2minTempFlowTrends = pgView(
  'mv_2min_temp_flow_trends',
  trendColumns
).existing();

export const mv2minWaterCutGorTrends = pgView(
  'mv_2min_water_cut_gor_trends',
  trendColumns
).existing();

export const vActiveAlarms = pgView('v_active_alarms', {
  eventId: bigint('event_id', { mode: 'number' }),
  triggeredAt: timestamp('triggered_at', {
    withTimezone: true,
    mode: 'string',
  }),
  severityLevel: varchar('severity_level', { length: 50 }),
  triggeredValue: doublePrecision('triggered_value'),
  wellheadId: integer('wellhead_id'),
  wellheadName: varchar('wellhead_name', { length: 255 }),
  locationName: varchar('location_name', { length: 255 }),
  fieldName: varchar('field_name', { length: 255 }),
  parameterCode: varchar('parameter_code', { length: 100 }),
  parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
  canonicalUnit: varchar('canonical_unit', { length: 50 }),
  operator: varchar({ length: 10 }),
  thresholdValue: doublePrecision('threshold_value'),
  alarmRuleId: integer('alarm_rule_id'),
}).existing();

/** Alarm counts bucketed at five minutes. Previously named `mv_daily_*`. */
export const mv5minAlarmCounts = pgView('mv_5min_alarm_counts', {
  bucketTime: timestamp('bucket_time', { withTimezone: true, mode: 'string' }),
  wellheadId: integer('wellhead_id'),
  wellheadName: varchar('wellhead_name', { length: 255 }),
  locationName: varchar('location_name', { length: 255 }),
  parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
  severityLevel: varchar('severity_level', { length: 50 }),
  alarmsTriggered: bigint('alarms_triggered', { mode: 'number' }),
}).existing();
