export type DashboardSummary = {
  totalWellheads: number;
  parametersTracked: number;
  activeAlarms: number;
  latestReadingAt: string | null;
};

export type LatestReading = {
  timestampUtc?: string | Date | null;
  rawValue?: number | null;
  wellheadId?: number | null;
  wellheadName?: string | null;
  locationName?: string | null;
  fieldName?: string | null;
  parameterCode?: string | null;
  parameterDisplayName?: string | null;
  canonicalUnit?: string | null;
  normalMin?: number | null;
  normalMax?: number | null;
};

export type ActiveAlarm = {
  eventId?: number | null;
  triggeredAt?: string | Date | null;
  severityLevel?: string | null;
  triggeredValue?: number | null;
  wellheadName?: string | null;
  parameterDisplayName?: string | null;
  thresholdValue?: number | null;
};

export type DashboardOverviewResponse = {
  summary: DashboardSummary;
  latestReadings: LatestReading[];
  activeAlarms: ActiveAlarm[];
};

export type TrendPoint = {
  bucketTime?: string | Date | null;
  parameterCode?: string | null;
  parameterDisplayName?: string | null;
  canonicalUnit?: string | null;
  avgValue?: number | null;
  minValue?: number | null;
  maxValue?: number | null;
  readingCount?: number | null;
};

export type DailyAlarmCount = {
  bucketDay?: string | Date | null;
  severityLevel?: string | null;
  totalAlarmsTriggered?: number | null;
};

export type DashboardAnalyticsResponse = {
  pressureTrend: TrendPoint[];
  temperatureFlowTrend: TrendPoint[];
  waterCutGorTrend: TrendPoint[];
  dailyAlarmCounts: DailyAlarmCount[];
};
