import { Hono } from 'hono';
import { and, asc, db, desc, eq, sql } from '@corsight/db/query';
import {
  field,
  location,
  parameterreading,
  parametertype,
  wellhead,
} from '@corsight/db/schemas/schema';
import type {
  DashboardAnalyticsResponse,
  DashboardOverviewResponse,
  WellheadDetailResponse,
} from '@corsight/dto/res/dashboard';
import {
  mvDailyAlarmCounts,
  mvHourlyPressureTrends,
  mvHourlyTempFlowTrends,
  mvHourlyWaterCutGorTrends,
  vActiveAlarms,
  vWellheadParameterReadings,
} from '@corsight/db/schemas/views';
import { responseHandler } from '../utils/handler';
import { HTTPException } from 'hono/http-exception';

const RECENT_READING_LIMIT = 24;
const ACTIVE_ALARM_LIMIT = 12;
const ANALYTICS_TREND_LIMIT = 144;

async function countRows(
  table: typeof wellhead | typeof parametertype | typeof vActiveAlarms
) {
  const [row] = await db
    .select({ value: sql<number>`count(*)::int` })
    .from(table);

  return row?.value ?? 0;
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

async function latestWellheadReadings(
  wellheadId: number,
  timestampUtc: string | null
) {
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
      bucketTime: mvHourlyPressureTrends.bucketTime,
      parameterCode: mvHourlyPressureTrends.parameterCode,
      parameterDisplayName: mvHourlyPressureTrends.parameterDisplayName,
      canonicalUnit: mvHourlyPressureTrends.canonicalUnit,
      avgValue: sql<number>`avg(${mvHourlyPressureTrends.avgValue})::float`,
      minValue: sql<number>`min(${mvHourlyPressureTrends.minValue})::float`,
      maxValue: sql<number>`max(${mvHourlyPressureTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mvHourlyPressureTrends.readingCount})::int`,
    })
    .from(mvHourlyPressureTrends)
    .groupBy(
      mvHourlyPressureTrends.bucketTime,
      mvHourlyPressureTrends.parameterCode,
      mvHourlyPressureTrends.parameterDisplayName,
      mvHourlyPressureTrends.canonicalUnit
    )
    .orderBy(desc(mvHourlyPressureTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function pressureTrendForWellhead(wellheadId: number) {
  return db
    .select({
      bucketTime: mvHourlyPressureTrends.bucketTime,
      parameterCode: mvHourlyPressureTrends.parameterCode,
      parameterDisplayName: mvHourlyPressureTrends.parameterDisplayName,
      canonicalUnit: mvHourlyPressureTrends.canonicalUnit,
      avgValue: mvHourlyPressureTrends.avgValue,
      minValue: mvHourlyPressureTrends.minValue,
      maxValue: mvHourlyPressureTrends.maxValue,
      readingCount: mvHourlyPressureTrends.readingCount,
    })
    .from(mvHourlyPressureTrends)
    .where(eq(mvHourlyPressureTrends.wellheadId, wellheadId))
    .orderBy(desc(mvHourlyPressureTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function tempFlowTrend() {
  return db
    .select({
      bucketTime: mvHourlyTempFlowTrends.bucketTime,
      parameterCode: mvHourlyTempFlowTrends.parameterCode,
      parameterDisplayName: mvHourlyTempFlowTrends.parameterDisplayName,
      canonicalUnit: mvHourlyTempFlowTrends.canonicalUnit,
      avgValue: sql<number>`avg(${mvHourlyTempFlowTrends.avgValue})::float`,
      minValue: sql<number>`min(${mvHourlyTempFlowTrends.minValue})::float`,
      maxValue: sql<number>`max(${mvHourlyTempFlowTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mvHourlyTempFlowTrends.readingCount})::int`,
    })
    .from(mvHourlyTempFlowTrends)
    .groupBy(
      mvHourlyTempFlowTrends.bucketTime,
      mvHourlyTempFlowTrends.parameterCode,
      mvHourlyTempFlowTrends.parameterDisplayName,
      mvHourlyTempFlowTrends.canonicalUnit
    )
    .orderBy(desc(mvHourlyTempFlowTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function tempFlowTrendForWellhead(wellheadId: number) {
  return db
    .select({
      bucketTime: mvHourlyTempFlowTrends.bucketTime,
      parameterCode: mvHourlyTempFlowTrends.parameterCode,
      parameterDisplayName: mvHourlyTempFlowTrends.parameterDisplayName,
      canonicalUnit: mvHourlyTempFlowTrends.canonicalUnit,
      avgValue: mvHourlyTempFlowTrends.avgValue,
      minValue: mvHourlyTempFlowTrends.minValue,
      maxValue: mvHourlyTempFlowTrends.maxValue,
      readingCount: mvHourlyTempFlowTrends.readingCount,
    })
    .from(mvHourlyTempFlowTrends)
    .where(eq(mvHourlyTempFlowTrends.wellheadId, wellheadId))
    .orderBy(desc(mvHourlyTempFlowTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function waterCutGorTrend() {
  return db
    .select({
      bucketTime: mvHourlyWaterCutGorTrends.bucketTime,
      parameterCode: mvHourlyWaterCutGorTrends.parameterCode,
      parameterDisplayName: mvHourlyWaterCutGorTrends.parameterDisplayName,
      canonicalUnit: mvHourlyWaterCutGorTrends.canonicalUnit,
      avgValue: sql<number>`avg(${mvHourlyWaterCutGorTrends.avgValue})::float`,
      minValue: sql<number>`min(${mvHourlyWaterCutGorTrends.minValue})::float`,
      maxValue: sql<number>`max(${mvHourlyWaterCutGorTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mvHourlyWaterCutGorTrends.readingCount})::int`,
    })
    .from(mvHourlyWaterCutGorTrends)
    .groupBy(
      mvHourlyWaterCutGorTrends.bucketTime,
      mvHourlyWaterCutGorTrends.parameterCode,
      mvHourlyWaterCutGorTrends.parameterDisplayName,
      mvHourlyWaterCutGorTrends.canonicalUnit
    )
    .orderBy(desc(mvHourlyWaterCutGorTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function waterCutGorTrendForWellhead(wellheadId: number) {
  return db
    .select({
      bucketTime: mvHourlyWaterCutGorTrends.bucketTime,
      parameterCode: mvHourlyWaterCutGorTrends.parameterCode,
      parameterDisplayName: mvHourlyWaterCutGorTrends.parameterDisplayName,
      canonicalUnit: mvHourlyWaterCutGorTrends.canonicalUnit,
      avgValue: mvHourlyWaterCutGorTrends.avgValue,
      minValue: mvHourlyWaterCutGorTrends.minValue,
      maxValue: mvHourlyWaterCutGorTrends.maxValue,
      readingCount: mvHourlyWaterCutGorTrends.readingCount,
    })
    .from(mvHourlyWaterCutGorTrends)
    .where(eq(mvHourlyWaterCutGorTrends.wellheadId, wellheadId))
    .orderBy(desc(mvHourlyWaterCutGorTrends.bucketTime))
    .limit(ANALYTICS_TREND_LIMIT);
}

async function dailyAlarmCounts() {
  return db
    .select()
    .from(mvDailyAlarmCounts)
    .orderBy(desc(mvDailyAlarmCounts.bucketDay))
    .limit(ANALYTICS_TREND_LIMIT);
}

export const dashboardRouter = new Hono()
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
      const [
        readings,
        alarms,
        pressure,
        temperatureFlow,
        waterCutGor,
      ] = await Promise.all([
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
      const [pressure, temperatureFlow, waterCutGor, alarmsByDay] =
        await Promise.all([
          pressureTrend(),
          tempFlowTrend(),
          waterCutGorTrend(),
          dailyAlarmCounts(),
        ]);

      const analytics: DashboardAnalyticsResponse = {
        pressureTrend: pressure,
        temperatureFlowTrend: temperatureFlow,
        waterCutGorTrend: waterCutGor,
        dailyAlarmCounts: alarmsByDay,
      };

      return analytics;
    })(c)
  );
