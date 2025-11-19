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
} from 'drizzle-orm/pg-core';

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
