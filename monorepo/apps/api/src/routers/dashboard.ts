import { Hono } from 'hono';
import { and, asc, db, desc, eq, sql } from '@well-simulation/db/query';
import {
  field,
  location,
  parameterreading,
  parametertype,
  wellhead,
} from '@well-simulation/db/schemas/schema';
import type {
  DashboardAnalyticsResponse,
  DashboardOverviewResponse,
  WellheadDetailResponse,
} from '@well-simulation/dto/res/dashboard';
import {
  mv5minAlarmCounts,
  mv2minPressureTrends,
  mv2minTempFlowTrends,
  mv2minWaterCutGorTrends,
  vActiveAlarms,
  vWellheadParameterReadings,
} from '@well-simulation/db/schemas/views';
import { responseHandler } from '../utils/handler';
import { requireCapability } from '../middleware/authorize';
import { HTTPException } from 'hono/http-exception';

const RECENT_READING_LIMIT = 24;
const ACTIVE_ALARM_LIMIT = 12;
const ANALYTICS_TREND_LIMIT = 144;

async function countRows(
  table: typeof wellhead | typeof parametertype | typeof vActiveAlarms
) {
  const [row] = await db.select({ value: sql<number>`count(*)::int` }).from(table);

  return row?.value ?? 0;
}

/**
 * Seconds since a timestamp, or null when there is none.
 *
 * Clamped at zero: a reading dated slightly in the future, which clock skew
 * between the ingestion container and the API can produce, should read as
 * fresh rather than as a negative age.
 */
function ageInSeconds(timestampUtc: string | null): number | null {
  if (!timestampUtc) return null;
  const age = (Date.now() - new Date(timestampUtc).getTime()) / 1000;
  return Math.max(0, Math.round(age));
}

async function latestReadingTimestamp() {
  const [row] = await db
    .select({ value: sql<string | null>`max(${parameterreading.timestampUtc})` })
    .from(parameterreading);

  return row?.value ?? null;
}

async function latestReadings(limit = RECENT_READING_LIMIT) {
  return db
    .select()
    .from(vWellheadParameterReadings)
    .orderBy(desc(vWellheadParameterReadings.timestampUtc))
    .limit(limit);
}

async function latestSnapshotReadings(timestampUtc: string | null) {
  if (!timestampUtc) {
    return [];
  }

  return db
    .select()
    .from(vWellheadParameterReadings)
    .where(eq(vWellheadParameterReadings.timestampUtc, timestampUtc))
    .orderBy(
      asc(vWellheadParameterReadings.wellheadId),
      asc(vWellheadParameterReadings.parameterTypeId)
    );
}

async function wellheadAsset(wellheadId: number) {
  const [asset] = await db
    .select({
      wellheadId: wellhead.wellheadId,
      wellheadName: wellhead.name,
      wellheadType: wellhead.type,
      status: wellhead.status,
      locationId: location.locationId,
      locationName: location.name,
      fieldId: field.fieldId,
      fieldName: field.name,
    })
    .from(wellhead)
    .innerJoin(location, eq(wellhead.locationId, location.locationId))
    .innerJoin(field, eq(location.fieldId, field.fieldId))
    .where(eq(wellhead.wellheadId, wellheadId))
    .limit(1);

  return asset ?? null;
}

async function latestWellheadReadingTimestamp(wellheadId: number) {
  const [row] = await db
    .select({ value: sql<string | null>`max(${parameterreading.timestampUtc})` })
    .from(parameterreading)
    .where(eq(parameterreading.wellheadId, wellheadId));

  return row?.value ?? null;
}

async function latestWellheadReadings(wellheadId: number, timestampUtc: string | null) {
  if (!timestampUtc) {
    return [];
  }

  return db
    .select()
    .from(vWellheadParameterReadings)
    .where(
      and(
        eq(vWellheadParameterReadings.wellheadId, wellheadId),
        eq(vWellheadParameterReadings.timestampUtc, timestampUtc)
      )
    )
    .orderBy(asc(vWellheadParameterReadings.parameterTypeId));
}

async function activeAlarms(limit = ACTIVE_ALARM_LIMIT) {
  return db
    .select()
    .from(vActiveAlarms)
    .orderBy(desc(vActiveAlarms.triggeredAt))
    .limit(limit);
}

async function activeAlarmsForWellhead(wellheadId: number) {
  return db
    .select()
    .from(vActiveAlarms)
    .where(eq(vActiveAlarms.wellheadId, wellheadId))
    .orderBy(desc(vActiveAlarms.triggeredAt))
    .limit(ACTIVE_ALARM_LIMIT);
}

async function pressureTrend() {
  return db
    .select({
      bucketTime: mv2minPressureTrends.bucketTime,
      parameterCode: mv2minPressureTrends.parameterCode,
      parameterDisplayName: mv2minPressureTrends.parameterDisplayName,
      canonicalUnit: mv2minPressureTrends.canonicalUnit,
      avgValue: sql<number>`avg(${mv2minPressureTrends.avgValue})::float`,
      minValue: sql<number>`min(${mv2minPressureTrends.minValue})::float`,
      maxValue: sql<number>`max(${mv2minPressureTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mv2minPressureTrends.readingCount})::int`,
    })
    .from(mv2minPressureTrends)
    .groupBy(
      mv2minPressureTrends.bucketTime,
      mv2minPressureTrends.parameterCode,
      mv2minPressureTrends.parameterDisplayName,
      mv2minPressureTrends.canonicalUnit
    )
    .orderBy(desc(mv2minPressureTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function pressureTrendForWellhead(wellheadId: number) {
  return db
    .select({
      bucketTime: mv2minPressureTrends.bucketTime,
      parameterCode: mv2minPressureTrends.parameterCode,
      parameterDisplayName: mv2minPressureTrends.parameterDisplayName,
      canonicalUnit: mv2minPressureTrends.canonicalUnit,
      avgValue: mv2minPressureTrends.avgValue,
      minValue: mv2minPressureTrends.minValue,
      maxValue: mv2minPressureTrends.maxValue,
      readingCount: mv2minPressureTrends.readingCount,
    })
    .from(mv2minPressureTrends)
    .where(eq(mv2minPressureTrends.wellheadId, wellheadId))
    .orderBy(desc(mv2minPressureTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function tempFlowTrend() {
  return db
    .select({
      bucketTime: mv2minTempFlowTrends.bucketTime,
      parameterCode: mv2minTempFlowTrends.parameterCode,
      parameterDisplayName: mv2minTempFlowTrends.parameterDisplayName,
      canonicalUnit: mv2minTempFlowTrends.canonicalUnit,
      avgValue: sql<number>`avg(${mv2minTempFlowTrends.avgValue})::float`,
      minValue: sql<number>`min(${mv2minTempFlowTrends.minValue})::float`,
      maxValue: sql<number>`max(${mv2minTempFlowTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mv2minTempFlowTrends.readingCount})::int`,
    })
    .from(mv2minTempFlowTrends)
    .groupBy(
      mv2minTempFlowTrends.bucketTime,
      mv2minTempFlowTrends.parameterCode,
      mv2minTempFlowTrends.parameterDisplayName,
      mv2minTempFlowTrends.canonicalUnit
    )
    .orderBy(desc(mv2minTempFlowTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function tempFlowTrendForWellhead(wellheadId: number) {
  return db
    .select({
      bucketTime: mv2minTempFlowTrends.bucketTime,
      parameterCode: mv2minTempFlowTrends.parameterCode,
      parameterDisplayName: mv2minTempFlowTrends.parameterDisplayName,
      canonicalUnit: mv2minTempFlowTrends.canonicalUnit,
      avgValue: mv2minTempFlowTrends.avgValue,
      minValue: mv2minTempFlowTrends.minValue,
      maxValue: mv2minTempFlowTrends.maxValue,
      readingCount: mv2minTempFlowTrends.readingCount,
    })
    .from(mv2minTempFlowTrends)
    .where(eq(mv2minTempFlowTrends.wellheadId, wellheadId))
    .orderBy(desc(mv2minTempFlowTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function waterCutGorTrend() {
  return db
    .select({
      bucketTime: mv2minWaterCutGorTrends.bucketTime,
      parameterCode: mv2minWaterCutGorTrends.parameterCode,
      parameterDisplayName: mv2minWaterCutGorTrends.parameterDisplayName,
      canonicalUnit: mv2minWaterCutGorTrends.canonicalUnit,
      avgValue: sql<number>`avg(${mv2minWaterCutGorTrends.avgValue})::float`,
      minValue: sql<number>`min(${mv2minWaterCutGorTrends.minValue})::float`,
      maxValue: sql<number>`max(${mv2minWaterCutGorTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mv2minWaterCutGorTrends.readingCount})::int`,
    })
    .from(mv2minWaterCutGorTrends)
    .groupBy(
      mv2minWaterCutGorTrends.bucketTime,
      mv2minWaterCutGorTrends.parameterCode,
      mv2minWaterCutGorTrends.parameterDisplayName,
      mv2minWaterCutGorTrends.canonicalUnit
    )
    .orderBy(desc(mv2minWaterCutGorTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function waterCutGorTrendForWellhead(wellheadId: number) {
  return db
    .select({
      bucketTime: mv2minWaterCutGorTrends.bucketTime,
      parameterCode: mv2minWaterCutGorTrends.parameterCode,
      parameterDisplayName: mv2minWaterCutGorTrends.parameterDisplayName,
      canonicalUnit: mv2minWaterCutGorTrends.canonicalUnit,
      avgValue: mv2minWaterCutGorTrends.avgValue,
      minValue: mv2minWaterCutGorTrends.minValue,
      maxValue: mv2minWaterCutGorTrends.maxValue,
      readingCount: mv2minWaterCutGorTrends.readingCount,
    })
    .from(mv2minWaterCutGorTrends)
    .where(eq(mv2minWaterCutGorTrends.wellheadId, wellheadId))
    .orderBy(desc(mv2minWaterCutGorTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function alarmCounts() {
  return db
    .select()
    .from(mv5minAlarmCounts)
    .orderBy(desc(mv5minAlarmCounts.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

export const dashboardRouter = new Hono()
  // Every dashboard read declares the capability it needs rather than relying
  // on authentication alone, so ADR 0033's model governs these routes too.
  .use('*', requireCapability('dashboard:read'))
  .get('/overview', (c) =>
    responseHandler(async () => {
      const [totalWellheads, parametersTracked, activeAlarmCount, latestAt] =
        await Promise.all([
          countRows(wellhead),
          countRows(parametertype),
          countRows(vActiveAlarms),
          latestReadingTimestamp(),
        ]);

      const [readings, alarms] = await Promise.all([
        latestSnapshotReadings(latestAt),
        activeAlarms(),
      ]);

      const overview: DashboardOverviewResponse = {
        summary: {
          totalWellheads,
          parametersTracked,
          activeAlarms: activeAlarmCount,
          latestReadingAt: latestAt,
          latestReadingAgeSeconds: ageInSeconds(latestAt),
        },
        latestReadings: readings,
        activeAlarms: alarms,
      };

      return overview;
    })(c)
  )
  .get('/latest-readings', (c) =>
    responseHandler(async () => {
      return { readings: await latestReadings(50) };
    })(c)
  )
  .get('/active-alarms', (c) =>
    responseHandler(async () => {
      return { alarms: await activeAlarms(50) };
    })(c)
  )
  .get('/wellheads/:wellheadId', (c) =>
    responseHandler(async () => {
      const wellheadId = Number.parseInt(c.req.param('wellheadId'), 10);

      if (!Number.isInteger(wellheadId) || wellheadId < 1) {
        throw new HTTPException(400, { message: 'Invalid wellhead id' });
      }

      const asset = await wellheadAsset(wellheadId);
      if (!asset) {
        throw new HTTPException(404, { message: 'Wellhead not found' });
      }

      const latestAt = await latestWellheadReadingTimestamp(wellheadId);
      const [readings, alarms, pressure, temperatureFlow, waterCutGor] =
        await Promise.all([
          latestWellheadReadings(wellheadId, latestAt),
          activeAlarmsForWellhead(wellheadId),
          pressureTrendForWellhead(wellheadId),
          tempFlowTrendForWellhead(wellheadId),
          waterCutGorTrendForWellhead(wellheadId),
        ]);

      const detail: WellheadDetailResponse = {
        wellhead: asset,
        latestReadingAt: latestAt,
        latestReadings: readings,
        activeAlarms: alarms,
        pressureTrend: pressure,
        temperatureFlowTrend: temperatureFlow,
        waterCutGorTrend: waterCutGor,
      };

      return detail;
    })(c)
  )
  .get('/analytics', (c) =>
    responseHandler(async () => {
      const [pressure, temperatureFlow, waterCutGor, alarmsByBucket] =
        await Promise.all([
          pressureTrend(),
          tempFlowTrend(),
          waterCutGorTrend(),
          alarmCounts(),
        ]);

      const analytics: DashboardAnalyticsResponse = {
        pressureTrend: pressure,
        temperatureFlowTrend: temperatureFlow,
        waterCutGorTrend: waterCutGor,
        alarmCounts: alarmsByBucket,
      };

      return analytics;
    })(c)
  );
