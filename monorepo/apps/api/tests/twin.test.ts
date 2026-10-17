import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { deleteTestUsers, request, signedInAs, withDb } from './helpers/api';

import type {
  ProjectionDetail,
  RunHistory,
} from '@well-simulation/dto/res/projection-detail';
import type { FleetModelStatus } from '@well-simulation/dto/res/model-status';
import type { DashboardOverviewResponse } from '@well-simulation/dto/res/dashboard';

/** Read the standard test envelope; each test asserts the fields it exercises. */
async function responseData<T>(response: Response): Promise<T> {
  return ((await response.json()) as { data: T }).data;
}

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
                 300, 300, '{}', '{}', '{"choke_position_percent":100}',
                 '[{"offset_seconds":0,"change":{"choke_position_percent":80,"pump_on":null}}]', '[]', 0.5, '["Synthetic model"]')`,
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
  it('returns the saved schedule and canonical units without point queries in history', async () => {
    const saved = await request(`/twin/scenarios/${scenarioId}`, { cookie });
    const result = await responseData<ProjectionDetail>(saved);
    expect(result.baselineControls).toEqual({ choke_position_percent: 100 });
    expect(result.scheduledControls).toEqual([
      { offsetMinutes: 0, input: { chokePositionPercent: 80 } },
    ]);
    expect(result.signals).toContainEqual(
      expect.objectContaining({ code: 'flow_rate', canonicalUnit: 'bbl/day' })
    );
    const history = await request('/twin/wellheads/1/runs?kind=scenario&limit=1', {
      cookie,
    });
    expect(history.status).toBe(200);
    const page = await responseData<RunHistory>(history);
    expect(page.items[0]?.runId).toBe(scenarioId);
    expect(page.items[0]).not.toHaveProperty('points');
    expect(page.hasMore).toBe(false);
    expect(
      (await request('/twin/wellheads/1/runs?kind=scenario&offset=1', { cookie }))
        .status
    ).toBe(200);
    const otherWell = await request('/twin/wellheads/2/runs?kind=scenario', { cookie });
    expect((await responseData<RunHistory>(otherWell)).items).toEqual([]);
  });

  it('bounds history, validates the well, and requires authentication', async () => {
    expect((await request('/twin/wellheads/1/runs')).status).toBe(401);
    for (const query of [
      'limit=51',
      'offset=-1',
      'offset=10001',
      'kind=invalid',
      'unexpected=1',
    ]) {
      expect(
        (await request(`/twin/wellheads/1/runs?${query}`, { cookie })).status
      ).toBe(400);
    }
    expect((await request('/twin/wellheads/2147483647/runs', { cookie })).status).toBe(
      404
    );
  });

  it('keeps model lifecycle reads available during partial startup and strips extra fields', async () => {
    const status = {
      wellheadId: 1,
      modelVersion: '0.3.0',
      status: 'degraded',
      reason: 'model_step_failed',
      lastTickAt: baseAt,
      simulatedAt: baseAt,
      processAgeSeconds: 100,
      current: false,
      skippedTicks: 0,
      restoredFrom: null,
      lastRecoveryEvent: null,
      internalSecret: 'must-not-pass',
    };
    const call = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(
        new Response(
          JSON.stringify({ checkedAt: baseAt, available: true, wellheads: [status] })
        )
      );
    try {
      expect((await request('/twin/status')).status).toBe(401);
      const response = await request('/twin/status', { cookie });
      expect(response.status).toBe(200);
      const data = await responseData<FleetModelStatus>(response);
      expect(data.wellheads[0]?.current).toBe(false);
      expect(data.wellheads[0]).not.toHaveProperty('internalSecret');
      expect(String(call.mock.calls[0]?.[0])).toContain('/model-status');
    } finally {
      call.mockRestore();
    }
  });

  it('reports an unavailable model without failing the optional status panel', async () => {
    const call = vi
      .spyOn(globalThis, 'fetch')
      .mockRejectedValue(new Error('unreachable'));
    try {
      const response = await request('/twin/status', { cookie });
      expect(response.status).toBe(200);
      expect(await responseData<FleetModelStatus>(response)).toMatchObject({
        available: false,
        wellheads: [],
      });
    } finally {
      call.mockRestore();
    }
  });

  it('paginates metadata even when a run has no retained points', async () => {
    const secondId = randomUUID();
    await withDb((client) =>
      client.query(
        `INSERT INTO scenario_run
      SELECT $1, created_at + interval '1 second', wellhead_id, requested_by_user_id,
      name, model_version, parameter_fingerprint, base_simulated_at, horizon_seconds,
      step_seconds, base_state, parameter_values, control_values, scheduled_controls,
      alarm_rules, confidence_score, confidence_reasons FROM scenario_run WHERE run_id=$2`,
        [secondId, scenarioId]
      )
    );
    try {
      const first = await responseData<RunHistory>(
        await request('/twin/wellheads/1/runs?kind=scenario&limit=1', { cookie })
      );
      expect(first.items.map((item: { runId: string }) => item.runId)).toEqual([
        secondId,
      ]);
      expect(first.hasMore).toBe(true);
      const second = await responseData<RunHistory>(
        await request('/twin/wellheads/1/runs?kind=scenario&limit=1&offset=1', {
          cookie,
        })
      );
      expect(second.items.map((item: { runId: string }) => item.runId)).toEqual([
        scenarioId,
      ]);
      expect(second.hasMore).toBe(false);
      expect((await request(`/twin/scenarios/${secondId}`, { cookie })).status).toBe(
        410
      );
    } finally {
      await withDb((client) =>
        client.query('DELETE FROM scenario_run WHERE run_id=$1', [secondId])
      );
    }
  });

  it('includes configured wells without readings in the fleet overview', async () => {
    const expected = await withDb((client) =>
      client.query('SELECT wellhead_id FROM wellhead')
    );
    const response = await request('/dashboard/overview', { cookie });
    expect(response.status).toBe(200);
    const body = await responseData<DashboardOverviewResponse>(response);
    expect(body.wellheads).toHaveLength(expected.rows.length);
    expect(
      body.wellheads.every((asset: { activeAlarmCount: number }) =>
        Number.isInteger(asset.activeAlarmCount)
      )
    ).toBe(true);
  });
  it('authenticates strict snapshot searches and preserves exact identifiers', async () => {
    const at = new Date().toISOString();
    const path = `/twin/wellheads/1/snapshots?${new URLSearchParams({ at })}`;
    expect((await request(path)).status).toBe(401);
    expect((await request(path + '&extra=1', { cookie })).status).toBe(400);
    expect(
      (await request('/twin/wellheads/1/snapshots?at=invalid', { cookie })).status
    ).toBe(400);
    const payload = {
      wellheadId: 1,
      checkedAt: at,
      hasMore: false,
      items: [
        {
          snapshotId: '9007199254740993',
          simulatedAt: at,
          modelVersion: '0.3.0',
          compatible: true,
          reason: 'compatible',
        },
      ],
    };
    const call = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify(payload)));
    try {
      const response = await request(path, { cookie });
      expect(response.status).toBe(200);
      expect(await responseData(response)).toEqual(payload);
      const headers = new Headers(call.mock.calls[0]?.[1]?.headers);
      expect(headers.get('x-twin-internal-token')?.length).toBeGreaterThanOrEqual(32);
      call.mockResolvedValueOnce(
        new Response(JSON.stringify({ ...payload, wellheadId: 2 }))
      );
      expect((await request(path, { cookie })).status).toBe(503);
    } finally {
      call.mockRestore();
    }
  });

  it('keeps historical provenance in saved detail and history after source expiry', async () => {
    const origin = {
      kind: 'snapshot',
      snapshotId: '9007199254740993',
      simulatedAt: baseAt,
    };
    await withDb((client) =>
      client.query('UPDATE scenario_run SET base_state=$1 WHERE run_id=$2', [
        { branchOrigin: origin },
        scenarioId,
      ])
    );
    try {
      const saved = await responseData<ProjectionDetail>(
        await request(`/twin/scenarios/${scenarioId}`, { cookie })
      );
      expect(saved.branchOrigin).toEqual(origin);
      const history = await responseData<RunHistory>(
        await request('/twin/wellheads/1/runs?kind=scenario', { cookie })
      );
      expect(
        history.items.find((item) => item.runId === scenarioId)?.branchOrigin
      ).toEqual(origin);
      // No source snapshot exists: saved reads depend on the run's own base record.
    } finally {
      await withDb((client) =>
        client.query("UPDATE scenario_run SET base_state='{}' WHERE run_id=$1", [
          scenarioId,
        ])
      );
    }
  });

  it('passes snapshot identity unchanged and rejects a mismatched upstream source', async () => {
    const selection = { snapshotId: '9007199254740993', simulatedAt: baseAt };
    const input = {
      wellheadId: 1,
      name: 'Past pressure',
      horizonSeconds: 300,
      stepSeconds: 300,
      controls: [{ offsetMinutes: 0, input: { pumpOn: false } }],
      baseSnapshot: selection,
    };
    const payload = {
      runId,
      kind: 'scenario',
      wellheadId: 1,
      name: input.name,
      source: 'synthetic_reduced_order_model',
      fieldCalibrated: false,
      modelVersion: '0.3.0',
      baseSimulatedAt: baseAt,
      generatedAt: baseAt,
      horizonSeconds: 300,
      stepSeconds: 300,
      branchOrigin: { kind: 'snapshot', ...selection },
      confidence: {
        method: 'heuristic_uncalibrated',
        score: 0.4,
        label: 'moderate',
        reasons: ['historical_snapshot'],
      },
      points: [
        {
          elapsedSeconds: 300,
          timestamp: forecastAt,
          values: { flow_rate: 0 },
          baselineValues: { flow_rate: 100 },
          alarmRisk: 'none',
          baselineAlarmRisk: 'none',
          triggeredRuleIds: [],
        },
      ],
    };
    const call = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify(payload), { status: 201 }));
    try {
      expect(
        (await request('/twin/scenarios', { method: 'POST', cookie, body: input }))
          .status
      ).toBe(200);
      expect(JSON.parse(String(call.mock.calls[0]?.[1]?.body)).baseSnapshot).toEqual(
        selection
      );
      call.mockResolvedValueOnce(
        new Response(
          JSON.stringify({ ...payload, branchOrigin: { kind: 'current' } }),
          { status: 201 }
        )
      );
      expect(
        (await request('/twin/scenarios', { method: 'POST', cookie, body: input }))
          .status
      ).toBe(502);
      call.mockResolvedValueOnce(new Response('{}', { status: 409 }));
      expect(
        (await request('/twin/scenarios', { method: 'POST', cookie, body: input }))
          .status
      ).toBe(409);
      expect(
        (
          await request('/twin/wellheads/1/forecast', {
            method: 'POST',
            cookie,
            body: { baseSnapshot: selection },
          })
        ).status
      ).toBe(400);
    } finally {
      call.mockRestore();
    }
  });
});
