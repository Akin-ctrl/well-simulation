import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  ALARM_SEVERITIES,
  type ActiveAlarmsResponse,
  type DashboardAnalyticsResponse,
} from '@well-simulation/dto/res/dashboard';

import { deleteTestUsers, request, signedInAs, withDb } from './helpers/api';

/**
 * The Alarm Center's counts must cover every active alarm, even though its list
 * is capped.
 *
 * Test rows carry parameter_reading_id -1, which no real reading has, so the
 * suite removes exactly what it inserted.
 */

const TEST_READING_ID = -1;

async function clearTestAlarms(): Promise<void> {
  await withDb((client) =>
    client.query('DELETE FROM alarmEvent WHERE parameter_reading_id = $1', [
      TEST_READING_ID,
    ])
  );
}

/**
 * Open one alarm for every wellhead and rule pair, each a minute older than the
 * last, and one cleared alarm that must not be counted.
 */
async function openAlarmForEveryRule(): Promise<void> {
  await withDb(async (client) => {
    await client.query(
      `INSERT INTO alarmEvent (alarm_rule_id, parameter_reading_id, timestamp_utc,
                               wellhead_id, triggered_at, severity_level, triggered_value)
       SELECT ar.alarm_rule_id, $1, now(), wh.wellhead_id,
              now() - (row_number() OVER (ORDER BY wh.wellhead_id, ar.alarm_rule_id))
                      * interval '1 minute',
              ar.severity_level, ar.threshold_value
       FROM wellHead wh CROSS JOIN alarmRule ar`,
      [TEST_READING_ID]
    );
    await client.query(
      `INSERT INTO alarmEvent (alarm_rule_id, parameter_reading_id, timestamp_utc,
                               wellhead_id, triggered_at, cleared_at, severity_level,
                               triggered_value)
       SELECT alarm_rule_id, $1, now(), (SELECT min(wellhead_id) FROM wellHead),
              now() - interval '1 day', now(), severity_level, threshold_value
       FROM alarmRule ORDER BY alarm_rule_id LIMIT 1`,
      [TEST_READING_ID]
    );
  });
}

/** The counts the API should report, computed independently in SQL. */
async function expectedTotals() {
  return withDb(async (client) => {
    const result = await client.query<{
      total: number;
      critical: number;
      warning: number;
      affected: number;
    }>(
      `SELECT count(*)::int AS total,
              (count(*) FILTER (WHERE severity_level = 'CRITICAL'))::int AS critical,
              (count(*) FILTER (WHERE severity_level = 'WARNING'))::int AS warning,
              count(DISTINCT wellhead_id)::int AS affected
       FROM alarmEvent WHERE cleared_at IS NULL`
    );
    return result.rows[0]!;
  });
}

async function fetchAlarmCenter(cookie: string): Promise<ActiveAlarmsResponse> {
  const response = await request('/dashboard/active-alarms', { cookie });
  expect(response.status).toBe(200);
  const body = (await response.json()) as { data: ActiveAlarmsResponse };
  return body.data;
}

describe('Alarm Center data', () => {
  let cookie: string;

  beforeAll(async () => {
    await deleteTestUsers();
    await clearTestAlarms();
    ({ cookie } = await signedInAs('USER'));
  });

  afterAll(async () => {
    await clearTestAlarms();
    await deleteTestUsers();
  });

  it('reports zero totals and no rows when nothing is active', async () => {
    const expected = await expectedTotals();
    // The suite's database receives no telemetry, so no other alarm is open.
    expect(expected.total).toBe(0);

    const data = await fetchAlarmCenter(cookie);
    expect(data.totals).toEqual({
      total: 0,
      critical: 0,
      warning: 0,
      affectedWellheads: 0,
    });
    expect(data.alarms).toEqual([]);
  });

  it('counts every active alarm even when the list is capped', async () => {
    await openAlarmForEveryRule();
    const expected = await expectedTotals();
    // The cap must actually be exercised, or this test proves nothing.
    expect(expected.total).toBeGreaterThan(50);

    const data = await fetchAlarmCenter(cookie);
    expect(data.totals).toEqual({
      total: expected.total,
      critical: expected.critical,
      warning: expected.warning,
      affectedWellheads: expected.affected,
    });
    expect(data.alarms).toHaveLength(50);
  });

  it('ranks the list most severe first, then oldest first', async () => {
    const data = await fetchAlarmCenter(cookie);
    const rank = (severity: string | null | undefined) =>
      ALARM_SEVERITIES.indexOf(severity as (typeof ALARM_SEVERITIES)[number]);

    for (let index = 1; index < data.alarms.length; index += 1) {
      const before = data.alarms[index - 1]!;
      const after = data.alarms[index]!;
      expect(rank(before.severityLevel)).toBeLessThanOrEqual(rank(after.severityLevel));
      if (before.severityLevel === after.severityLevel) {
        expect(new Date(before.triggeredAt!).getTime()).toBeLessThanOrEqual(
          new Date(after.triggeredAt!).getTime()
        );
      }
    }

    // The cut keeps critical alarms: none is dropped while a warning is shown.
    const shownCritical = data.alarms.filter((a) => a.severityLevel === 'CRITICAL');
    expect(shownCritical).toHaveLength(Math.min(data.totals.critical, 50));
  });
});

describe('Analytics alarm severity totals', () => {
  let cookie: string;

  /** Open `count` alarms of one severity, `ageHours` hours ago. */
  async function openAlarms(severity: string, count: number, ageHours: number) {
    await withDb((client) =>
      client.query(
        `INSERT INTO alarmEvent (alarm_rule_id, parameter_reading_id, timestamp_utc,
                                 wellhead_id, triggered_at, severity_level, triggered_value)
         SELECT ar.alarm_rule_id, $1, now(), (SELECT min(wellhead_id) FROM wellHead),
                now() - make_interval(hours => $2) - make_interval(secs => n),
                ar.severity_level, ar.threshold_value
         FROM generate_series(1, $3) AS n,
              (SELECT * FROM alarmRule WHERE severity_level = $4
               ORDER BY alarm_rule_id LIMIT 1) AS ar`,
        [TEST_READING_ID, ageHours, count, severity]
      )
    );
  }

  /** Continuous aggregates update on a schedule; bring this one up to date now. */
  async function refreshAlarmCounts() {
    await withDb((client) =>
      client.query(
        "CALL refresh_continuous_aggregate('mv_5min_alarm_counts', NULL, NULL)"
      )
    );
  }

  beforeAll(async () => {
    await deleteTestUsers();
    await clearTestAlarms();
    ({ cookie } = await signedInAs('USER'));
  });

  afterAll(async () => {
    await clearTestAlarms();
    await refreshAlarmCounts();
    await deleteTestUsers();
  });

  it('counts only the last 24 hours, one total per severity', async () => {
    await openAlarms('CRITICAL', 3, 0);
    await openAlarms('WARNING', 2, 0);
    // Older than the window: must not reach the total.
    await openAlarms('CRITICAL', 4, 48);
    await refreshAlarmCounts();

    const response = await request('/dashboard/analytics', { cookie });
    expect(response.status).toBe(200);
    const body = (await response.json()) as { data: DashboardAnalyticsResponse };
    expect(body.data.alarmCounts).toEqual([
      { severityLevel: 'CRITICAL', alarmsTriggered: 3 },
      { severityLevel: 'WARNING', alarmsTriggered: 2 },
    ]);
  });
});
