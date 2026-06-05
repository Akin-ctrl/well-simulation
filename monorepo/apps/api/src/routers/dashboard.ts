import { Hono } from 'hono';
import { db, desc, sql } from '@corsight/db/query';
import {
  parameterreading,
  parametertype,
  wellhead,
} from '@corsight/db/schemas/schema';
import {
  mvDailyAlarmCounts,
  mvHourlyPressureTrends,
  mvHourlyTempFlowTrends,
  mvHourlyWaterCutGorTrends,
  vActiveAlarms,
  vWellheadParameterReadings,
} from '@corsight/db/schemas/views';
import { responseHandler } from '../utils/handler';

const RECENT_READING_LIMIT = 24;
const ACTIVE_ALARM_LIMIT = 12;
const ANALYTICS_TREND_LIMIT = 48;

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

async function activeAlarms(limit = ACTIVE_ALARM_LIMIT) {
  return db
    .select()
    .from(vActiveAlarms)
    .orderBy(desc(vActiveAlarms.triggeredAt))
    .limit(limit);
}

async function pressureTrend() {
  return db
    .select({
      bucketTime: mvHourlyPressureTrends.bucketTime,
      avgValue: sql<number>`avg(${mvHourlyPressureTrends.avgValue})::float`,
      minValue: sql<number>`min(${mvHourlyPressureTrends.minValue})::float`,
      maxValue: sql<number>`max(${mvHourlyPressureTrends.maxValue})::float`,
      readingCount: sql<number>`sum(${mvHourlyPressureTrends.readingCount})::int`,
    })
    .from(mvHourlyPressureTrends)
    .groupBy(mvHourlyPressureTrends.bucketTime)
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
        latestReadings(),
        activeAlarms(),
      ]);

      return {
        summary: {
          totalWellheads,
          parametersTracked,
          activeAlarms: activeAlarmCount,
          latestReadingAt: latestAt,
        },
        latestReadings: readings,
        activeAlarms: alarms,
      };
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
  .get('/analytics', (c) =>
    responseHandler(async () => {
      const [pressure, temperatureFlow, waterCutGor, alarmsByDay] =
        await Promise.all([
          pressureTrend(),
          tempFlowTrend(),
          waterCutGorTrend(),
          dailyAlarmCounts(),
        ]);

      return {
        pressureTrend: pressure,
        temperatureFlowTrend: temperatureFlow,
        waterCutGorTrend: waterCutGor,
        dailyAlarmCounts: alarmsByDay,
      };
    })(c)
  );
