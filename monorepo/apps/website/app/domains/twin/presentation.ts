import type { SnapshotSelection } from '@well-simulation/dto/req/snapshots';
import { ApiError } from '@well-simulation/dto/error';
import {
  booleanStateLabel,
  type LatestReading,
} from '@well-simulation/dto/res/dashboard';
import {
  SCENARIO_CONTROLS,
  type ScenarioControlKey,
} from '@well-simulation/dto/req/scenario-controls';
import { scenarioRequestSchema } from '@well-simulation/dto/req/projection';
import type { ProjectionDetail } from '@well-simulation/dto/res/projection-detail';

export function numberText(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value)
    ? 'Unavailable'
    : new Intl.NumberFormat('en', { maximumFractionDigits: 2 }).format(value);
}
export function timeText(value: string | Date | null | undefined): string {
  if (!value || !Number.isFinite(new Date(value).getTime())) return 'Unavailable';
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'medium',
  }).format(new Date(value));
}
export function signalText(
  code: string,
  value: number | null | undefined,
  unit?: string | null
): string {
  if (value == null) return 'Unavailable';
  return (
    booleanStateLabel(code, value) ?? `${numberText(value)}${unit ? ` ${unit}` : ''}`
  );
}
export function abnormal(reading: LatestReading): boolean {
  return (
    reading.rawValue != null &&
    reading.normalMin != null &&
    reading.normalMax != null &&
    (reading.rawValue < reading.normalMin || reading.rawValue > reading.normalMax)
  );
}
export function errorState(error: unknown): {
  status: number | undefined;
  message: string;
} {
  const status = error instanceof ApiError ? error.statusCode : undefined;
  const messages: Record<number, string> = {
    400: 'Check the requested values and try again.',
    401: 'Your session has ended. Sign in again.',
    403: 'Your account does not have permission for this action.',
    404: 'We could not find this well or saved result.',
    409: 'The selected snapshot no longer matches this model. Find and select another snapshot.',
    410: 'The saved result points have expired. Its history entry remains available.',
    429: 'Too many requests. Wait before trying again.',
    503: 'The model or database is busy or unavailable.',
    504: 'The request timed out. The server may still have saved the result.',
  };
  return {
    status,
    message:
      status && messages[status]
        ? messages[status]!
        : 'We could not complete this request.',
  };
}
export type ScheduleRow = {
  id: number;
  offsetMinutes: string;
  control: ScenarioControlKey;
  value: string;
};

/** The shared schema enforces bounds; this function only converts form strings. */
export function scenarioInput(
  wellheadId: number,
  name: string,
  horizonSeconds: number,
  stepSeconds: number,
  rows: ScheduleRow[],
  baseSnapshot?: SnapshotSelection
) {
  return scenarioRequestSchema.safeParse({
    wellheadId,
    name,
    baseSnapshot,
    horizonSeconds,
    stepSeconds,
    controls: rows.map((row) => ({
      offsetMinutes: row.offsetMinutes.trim() === '' ? NaN : Number(row.offsetMinutes),
      input: {
        [row.control]: SCENARIO_CONTROLS[row.control].numeric
          ? row.value.trim() === ''
            ? NaN
            : Number(row.value)
          : row.value === 'true'
            ? true
            : row.value === 'false'
              ? false
              : NaN,
      },
    })),
  });
}
export type ProjectionRow = {
  time: number;
  value: number | null;
  baseline: number | null;
};

/** Preserve missing values and the actual sample times, with explicit gap breaks. */
export function projectionRows(run: ProjectionDetail, code: string): ProjectionRow[] {
  const rows: ProjectionRow[] = [];
  for (const point of [...run.points].sort(
    (a, b) => a.elapsedSeconds - b.elapsedSeconds
  )) {
    const time = Date.parse(point.timestamp);
    if (!Number.isFinite(time)) continue;
    const previous = rows.at(-1);
    if (previous && time - previous.time > run.stepSeconds * 1500) {
      rows.push({
        time: previous.time + run.stepSeconds * 1000,
        value: null,
        baseline: null,
      });
    }
    rows.push({
      time,
      value: point.values[code] ?? null,
      baseline: point.baselineValues?.[code] ?? null,
    });
  }
  return rows;
}
export function controlText(key: string, value: number | boolean): string {
  if (typeof value === 'number')
    return `${numberText(value)}${key.toLowerCase().includes('percent') ? '%' : ''}`;
  if (key === 'emergencyShutdown' || key === 'emergency_shutdown')
    return value ? 'Tripped' : 'Normal';
  if (key === 'pumpOn' || key === 'pump_on') return value ? 'Running' : 'Stopped';
  return value ? 'Open' : 'Closed';
}
