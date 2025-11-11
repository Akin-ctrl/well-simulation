import { sql } from 'drizzle-orm';
import {
  varchar,
  integer,
  doublePrecision,
  timestamp,
  bigint,
  pgView,
} from 'drizzle-orm/pg-core';

export const vWellheadParameterReadings = pgView(
  'v_wellhead_parameter_readings',
  {
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
  }
).as(
  sql`SELECT pr.timestamp_utc, pr.raw_value, wh.wellhead_id, wh.name AS wellhead_name, wh.type AS wellhead_type, loc.location_id, loc.name AS location_name, f.field_id, f.name AS field_name, pt.parameter_type_id, pt.code AS parameter_code, pt.display_name AS parameter_display_name, pt.canonical_unit, pt.data_type, pt.normal_min, pt.normal_max FROM parameterreading pr JOIN wellhead wh ON pr.wellhead_id = wh.wellhead_id JOIN location loc ON wh.location_id = loc.location_id JOIN field f ON loc.field_id = f.field_id JOIN parametertype pt ON pr.parameter_type_id = pt.parameter_type_id`
);

export const mvHourlyPressureTrends = pgView('mv_hourly_pressure_trends', {
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
  // You can use { mode: "bigint" } if numbers are exceeding js number limitations
  readingCount: bigint('reading_count', { mode: 'number' }),
}).as(
  sql`SELECT _materialized_hypertable_3.bucket_time, _materialized_hypertable_3.wellhead_id, _materialized_hypertable_3.wellhead_name, _materialized_hypertable_3.location_name, _materialized_hypertable_3.field_name, _materialized_hypertable_3.parameter_code, _materialized_hypertable_3.parameter_display_name, _materialized_hypertable_3.canonical_unit, _materialized_hypertable_3.avg_value, _materialized_hypertable_3.min_value, _materialized_hypertable_3.max_value, _materialized_hypertable_3.reading_count FROM _timescaledb_internal._materialized_hypertable_3`
);

export const mvHourlyTempFlowTrends = pgView('mv_hourly_temp_flow_trends', {
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
  // You can use { mode: "bigint" } if numbers are exceeding js number limitations
  readingCount: bigint('reading_count', { mode: 'number' }),
}).as(
  sql`SELECT _materialized_hypertable_4.bucket_time, _materialized_hypertable_4.wellhead_id, _materialized_hypertable_4.wellhead_name, _materialized_hypertable_4.location_name, _materialized_hypertable_4.field_name, _materialized_hypertable_4.parameter_code, _materialized_hypertable_4.parameter_display_name, _materialized_hypertable_4.canonical_unit, _materialized_hypertable_4.avg_value, _materialized_hypertable_4.min_value, _materialized_hypertable_4.max_value, _materialized_hypertable_4.reading_count FROM _timescaledb_internal._materialized_hypertable_4`
);

export const mvHourlyWaterCutGorTrends = pgView(
  'mv_hourly_water_cut_gor_trends',
  {
    bucketTime: timestamp('bucket_time', {
      withTimezone: true,
      mode: 'string',
    }),
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
    // You can use { mode: "bigint" } if numbers are exceeding js number limitations
    readingCount: bigint('reading_count', { mode: 'number' }),
  }
).as(
  sql`SELECT _materialized_hypertable_5.bucket_time, _materialized_hypertable_5.wellhead_id, _materialized_hypertable_5.wellhead_name, _materialized_hypertable_5.location_name, _materialized_hypertable_5.field_name, _materialized_hypertable_5.parameter_code, _materialized_hypertable_5.parameter_display_name, _materialized_hypertable_5.canonical_unit, _materialized_hypertable_5.avg_value, _materialized_hypertable_5.min_value, _materialized_hypertable_5.max_value, _materialized_hypertable_5.reading_count FROM _timescaledb_internal._materialized_hypertable_5`
);

export const mvDailyAllParameterSummary = pgView(
  'mv_daily_all_parameter_summary',
  {
    bucketDay: timestamp('bucket_day', { withTimezone: true, mode: 'string' }),
    wellheadId: integer('wellhead_id'),
    wellheadName: varchar('wellhead_name', { length: 255 }),
    locationName: varchar('location_name', { length: 255 }),
    fieldName: varchar('field_name', { length: 255 }),
    parameterCode: varchar('parameter_code', { length: 100 }),
    parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
    canonicalUnit: varchar('canonical_unit', { length: 50 }),
    dailyAvgValue: doublePrecision('daily_avg_value'),
    dailyMinValue: doublePrecision('daily_min_value'),
    dailyMaxValue: doublePrecision('daily_max_value'),
    dailyStddevValue: doublePrecision('daily_stddev_value'),
    // You can use { mode: "bigint" } if numbers are exceeding js number limitations
    readingCount: bigint('reading_count', { mode: 'number' }),
  }
).as(
  sql`SELECT _materialized_hypertable_6.bucket_day, _materialized_hypertable_6.wellhead_id, _materialized_hypertable_6.wellhead_name, _materialized_hypertable_6.location_name, _materialized_hypertable_6.field_name, _materialized_hypertable_6.parameter_code, _materialized_hypertable_6.parameter_display_name, _materialized_hypertable_6.canonical_unit, _materialized_hypertable_6.daily_avg_value, _materialized_hypertable_6.daily_min_value, _materialized_hypertable_6.daily_max_value, _materialized_hypertable_6.daily_stddev_value, _materialized_hypertable_6.reading_count FROM _timescaledb_internal._materialized_hypertable_6`
);

export const vActiveAlarms = pgView('v_active_alarms', {
  // You can use { mode: "bigint" } if numbers are exceeding js number limitations
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
  parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
  operator: varchar({ length: 10 }),
  thresholdValue: doublePrecision('threshold_value'),
  alarmRuleId: integer('alarm_rule_id'),
}).as(
  sql`SELECT ae.event_id, ae.triggered_at, ae.severity_level, ae.triggered_value, wh.wellhead_id, wh.name AS wellhead_name, loc.name AS location_name, f.name AS field_name, pt.display_name AS parameter_display_name, ar.operator, ar.threshold_value, ar.alarm_rule_id FROM alarmevent ae JOIN wellhead wh ON ae.wellhead_id = wh.wellhead_id JOIN location loc ON wh.location_id = loc.location_id JOIN field f ON loc.field_id = f.field_id JOIN alarmrule ar ON ae.alarm_rule_id = ar.alarm_rule_id JOIN parametertype pt ON ar.parameter_type_id = pt.parameter_type_id WHERE ae.cleared_at IS NULL`
);

export const mvDailyAlarmCounts = pgView('mv_daily_alarm_counts', {
  bucketDay: timestamp('bucket_day', { withTimezone: true, mode: 'string' }),
  wellheadId: integer('wellhead_id'),
  wellheadName: varchar('wellhead_name', { length: 255 }),
  locationName: varchar('location_name', { length: 255 }),
  parameterDisplayName: varchar('parameter_display_name', { length: 255 }),
  severityLevel: varchar('severity_level', { length: 50 }),
  // You can use { mode: "bigint" } if numbers are exceeding js number limitations
  totalAlarmsTriggered: bigint('total_alarms_triggered', { mode: 'number' }),
}).as(
  sql`SELECT _materialized_hypertable_7.bucket_day, _materialized_hypertable_7.wellhead_id, _materialized_hypertable_7.wellhead_name, _materialized_hypertable_7.location_name, _materialized_hypertable_7.parameter_display_name, _materialized_hypertable_7.severity_level, _materialized_hypertable_7.total_alarms_triggered FROM _timescaledb_internal._materialized_hypertable_7`
);
