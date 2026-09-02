export type DashboardSummary = {
  totalWellheads: number;
  parametersTracked: number;
  activeAlarms: number;
  latestReadingAt: string | null;
  /**
   * How old the newest reading is, in seconds, measured on the server.
   *
   * Computed here rather than in the browser because a client clock that is
   * wrong would make stale data look fresh, which is the one thing the
   * freshness indicator exists to prevent. Null when nothing has been ingested.
   */
  latestReadingAgeSeconds: number | null;
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
  parameterCode?: string | null;
  parameterDisplayName?: string | null;
  canonicalUnit?: string | null;
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

export type AlarmCount = {
  bucketTime?: string | Date | null;
  severityLevel?: string | null;
  alarmsTriggered?: number | null;
};

export type DashboardAnalyticsResponse = {
  pressureTrend: TrendPoint[];
  temperatureFlowTrend: TrendPoint[];
  waterCutGorTrend: TrendPoint[];
  alarmCounts: AlarmCount[];
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
