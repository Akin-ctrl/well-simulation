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
  wellheadType?: string | null;
  locationId?: number | null;
  locationName?: string | null;
  fieldId?: number | null;
  fieldName?: string | null;
  parameterTypeId?: number | null;
  parameterCode?: string | null;
  parameterDisplayName?: string | null;
  canonicalUnit?: string | null;
  dataType?: string | null;
  normalMin?: number | null;
  normalMax?: number | null;
};

export type ActiveAlarm = {
  eventId?: number | null;
  triggeredAt?: string | Date | null;
  severityLevel?: string | null;
  triggeredValue?: number | null;
  wellheadId?: number | null;
  wellheadName?: string | null;
  locationName?: string | null;
  fieldName?: string | null;
  parameterDisplayName?: string | null;
  operator?: string | null;
  thresholdValue?: number | null;
  alarmRuleId?: number | null;
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

export type WellheadAsset = {
  wellheadId: number;
  wellheadName: string;
  wellheadType: string | null;
  status: string | null;
  locationId: number;
  locationName: string;
  fieldId: number;
  fieldName: string;
};

export type WellheadDetailResponse = {
  wellhead: WellheadAsset;
  latestReadingAt: string | null;
  latestReadings: LatestReading[];
  activeAlarms: ActiveAlarm[];
  pressureTrend: TrendPoint[];
  temperatureFlowTrend: TrendPoint[];
  waterCutGorTrend: TrendPoint[];
};
