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

/**
 * Words for on/off signals, which the historian stores as 0 or 1 with the unit
 * "state". The first word is shown for 0 and the second for 1.
 */
export const BOOLEAN_STATE_LABELS: Readonly<Record<string, readonly [string, string]>> =
  {
    master_valve_status: ['Closed', 'Open'],
    wing_valve_status: ['Closed', 'Open'],
    swab_valve_status: ['Closed', 'Open'],
    pump_status: ['Stopped', 'Running'],
    emergency_shutdown: ['Normal', 'Tripped'],
  };

/**
 * The word for an on/off reading, or null when the signal is not an on/off
 * signal or the value is not exactly 0 or 1. A null result means the caller
 * shows the raw number, so an unexpected value is never hidden behind a word.
 */
export function booleanStateLabel(
  parameterCode: string | null | undefined,
  value: number | null | undefined
): string | null {
  const labels = parameterCode ? BOOLEAN_STATE_LABELS[parameterCode] : undefined;
  if (!labels || (value !== 0 && value !== 1)) return null;
  return labels[value];
}

/**
 * Alarm severities, most urgent first.
 *
 * The database stores severity as free text, so this list is the one place
 * that says which levels exist and in what order an operator should deal with
 * them. The API sorts by it and the Alarm Center relies on that order. A level
 * that is not listed here sorts after all of these.
 */
export const ALARM_SEVERITIES = ['CRITICAL', 'WARNING'] as const;

export type AlarmSeverity = (typeof ALARM_SEVERITIES)[number];

/** Counts over every active alarm, not only the ones returned in the list. */
export type ActiveAlarmTotals = {
  total: number;
  critical: number;
  warning: number;
  affectedWellheads: number;
};

/**
 * The Alarm Center's data.
 *
 * `alarms` is capped, most severe first and then oldest first, so it can hold
 * fewer rows than `totals.total`. Counts must come from `totals`.
 */
export type ActiveAlarmsResponse = {
  totals: ActiveAlarmTotals;
  alarms: ActiveAlarm[];
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

/** Alarms triggered in the last 24 hours for one severity. */
export type AlarmCount = {
  severityLevel: string | null;
  alarmsTriggered: number;
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
