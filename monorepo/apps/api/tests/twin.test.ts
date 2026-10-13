import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { deleteTestUsers, request, signedInAs, withDb } from './helpers/api';

const runId = randomUUID();
const scenarioId = randomUUID();
const baseAt = new Date().toISOString();
const forecastAt = new Date(Date.now() + 300_000).toISOString();

describe('Milestone 8 projection API', () => {
  let cookie: string;
  let actor: number;

  beforeAll(async () => {
    await deleteTestUsers();
    const session = await signedInAs('USER');
    cookie = session.cookie;
    actor = session.user.id;
  });

  afterAll(async () => {
    vi.restoreAllMocks();
    await withDb(async (client) => {
      await client.query('DELETE FROM prediction_point WHERE run_id = $1', [runId]);
      await client.query('DELETE FROM prediction_run WHERE run_id = $1', [runId]);
      await client.query('DELETE FROM scenario_point WHERE run_id = $1', [scenarioId]);
      await client.query('DELETE FROM scenario_run WHERE run_id = $1', [scenarioId]);
    });
    await deleteTestUsers();
  });

  it('requires a session and validates bounded requests', async () => {
    const unsigned = await request('/twin/wellheads/1/forecast', {
      method: 'POST',
      body: {},
    });
    expect(unsigned.status).toBe(401);

    const invalid = await request('/twin/wellheads/1/forecast', {
      method: 'POST',
      cookie,
      body: { horizonSeconds: 7201 },
    });
    expect(invalid.status).toBe(400);

    const oversized = await request('/twin/wellheads/1/forecast', {
      method: 'POST',
      cookie,
      body: { padding: 'x'.repeat(5000) },
    });
    expect(oversized.status).toBe(413);
  });

  it('passes only the server-side token and actor to twin-core', async () => {
    const payload = {
      runId,
      kind: 'forecast',
      wellheadId: 1,
      name: null,
      source: 'synthetic_reduced_order_model',
      fieldCalibrated: false,
      modelVersion: '1.0.0',
      baseSimulatedAt: baseAt,
      generatedAt: baseAt,
      horizonSeconds: 300,
      stepSeconds: 300,
      confidence: {
        method: 'heuristic_uncalibrated',
        score: 0.55,
        label: 'moderate',
        reasons: ['Synthetic model'],
      },
      points: [
        {
          elapsedSeconds: 300,
          timestamp: forecastAt,
          values: { flow_rate: 100 },
          alarmRisk: 'none',
          triggeredRuleIds: [],
        },
      ],
    };
    const call = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify(payload), { status: 201 }));

    const response = await request('/twin/wellheads/1/forecast', {
      method: 'POST',
      cookie,
      body: { horizonSeconds: 300, stepSeconds: 300 },
    });
    expect(response.status).toBe(200);
    const body = (await response.json()) as { data: typeof payload };
    expect(body.data.runId).toBe(runId);
    const [url, init] = call.mock.calls[0]!;
    expect(String(url)).toContain('/wellheads/1/forecast');
    const headers = new Headers(init?.headers);
    expect(headers.get('x-twin-actor-id')).toBe(String(actor));
    expect(headers.get('x-twin-internal-token')?.length).toBeGreaterThanOrEqual(32);
    expect(JSON.stringify(body)).not.toContain(headers.get('x-twin-internal-token'));
    call.mockRestore();
  });

  it('loads a complete saved forecast and rejects partially expired points', async () => {
    await withDb(async (client) => {
      await client.query(
        `INSERT INTO prediction_run
         (run_id, created_at, wellhead_id, requested_by_user_id, model_version,
          parameter_fingerprint, base_simulated_at, horizon_seconds, step_seconds,
          base_state, parameter_values, control_values, alarm_rules,
          confidence_score, confidence_reasons)
         VALUES ($1, $2, 1, $3, '1.0.0', 'fingerprint', $2, 300, 300,
                 '{}', '{}', '{}', '[]', 0.55, '["Synthetic model"]')`,
        [runId, baseAt, actor]
      );
      await client.query(
        `INSERT INTO prediction_point
         (run_id, forecast_at, elapsed_seconds, values_json,
          tubing_pressure_psi, flow_rate_bpd, wellhead_temperature_f,
          water_cut_percent, alarm_risk, triggered_rule_ids)
         VALUES ($1, $2, 300, '{"flow_rate":100}', 1800, 100, 140, 20,
                 'none', '[]')`,
        [runId, forecastAt]
      );
    });

    const response = await request(`/twin/forecasts/${runId}`, { cookie });
    expect(response.status).toBe(200);
    const body = (await response.json()) as { data: { points: unknown[] } };
    expect(body.data.points).toHaveLength(1);

    await withDb((client) =>
      client.query('DELETE FROM prediction_point WHERE run_id = $1', [runId])
    );
    const expired = await request(`/twin/forecasts/${runId}`, { cookie });
    expect(expired.status).toBe(410);
  });

  it('loads the saved scenario and its unchanged baseline', async () => {
    await withDb(async (client) => {
      await client.query(
        `INSERT INTO scenario_run
         (run_id, created_at, wellhead_id, requested_by_user_id, name,
          model_version, parameter_fingerprint, base_simulated_at,
          horizon_seconds, step_seconds, base_state, parameter_values,
          control_values, scheduled_controls, alarm_rules,
          confidence_score, confidence_reasons)
         VALUES ($1, $2, 1, $3, 'Close choke', '1.0.0', 'fingerprint', $2,
                 300, 300, '{}', '{}', '{}', '[]', '[]', 0.5, '["Synthetic model"]')`,
        [scenarioId, baseAt, actor]
      );
      await client.query(
        `INSERT INTO scenario_point
         (run_id, forecast_at, elapsed_seconds, values_json,
          tubing_pressure_psi, flow_rate_bpd, wellhead_temperature_f,
          water_cut_percent, baseline_values_json, alarm_risk,
          baseline_alarm_risk, triggered_rule_ids)
         VALUES ($1, $2, 300, '{"flow_rate":0}', 2000, 0, 140, 20,
                 '{"flow_rate":100}', 'warning', 'none', '[]')`,
        [scenarioId, forecastAt]
      );
    });

    const response = await request(`/twin/scenarios/${scenarioId}`, { cookie });
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      data: {
        kind: string;
        points: {
          values: { flow_rate: number };
          baselineValues: { flow_rate: number };
        }[];
      };
    };
    expect(body.data.kind).toBe('scenario');
    expect(body.data.points[0]?.values.flow_rate).toBe(0);
    expect(body.data.points[0]?.baselineValues.flow_rate).toBe(100);
  });
});
