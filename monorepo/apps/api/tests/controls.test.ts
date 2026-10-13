import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { deleteTestUsers, request, signedInAs } from './helpers/api';
import { withDb } from './helpers/api';
import type {
  controlCommand,
  modelParameterChange,
} from '@well-simulation/db/schemas/controls';

type CommandResult = typeof controlCommand.$inferSelect & {
  notificationPending?: boolean;
};
type ParameterResult = typeof modelParameterChange.$inferSelect & {
  restartRequired?: boolean;
};

async function responseData<T>(response: Response): Promise<T> {
  return ((await response.json()) as { data: T }).data;
}

const state = {
  wellheadId: 1,
  revision: 0,
  modelVersion: '0.3.0',
  parameterFingerprint: 'a'.repeat(64),
  status: 'running',
  lastTickAt: new Date().toISOString(),
  controls: {
    chokePositionPercent: 100,
    masterValveOpen: true,
    wingValveOpen: true,
    swabValveOpen: false,
    pumpOn: true,
    emergencyShutdown: false,
  },
};
const draftBody = {
  wellheadId: 1,
  expectedRevision: 0,
  reason: 'Synthetic test',
  targets: { pumpOn: false },
};

async function clearCommands() {
  await withDb(async (client) => {
    await client.query(
      'DELETE FROM wellhead_control_state; DELETE FROM control_command_event; DELETE FROM control_command; DELETE FROM model_parameter_change; DELETE FROM wellhead_model_parameter'
    );
    await client.query(
      `INSERT INTO wellhead_control_state(wellhead_id,model_version,parameter_fingerprint,choke_position_percent,master_valve_open,wing_valve_open,swab_valve_open,pump_on,emergency_shutdown) VALUES(1,'0.3.0',$1,100,true,true,false,true,false)`,
      ['a'.repeat(64)]
    );
  });
}

describe('durable synthetic control API', () => {
  let operator: Awaited<ReturnType<typeof signedInAs>>;
  let administrator: Awaited<ReturnType<typeof signedInAs>>;
  let reader: Awaited<ReturnType<typeof signedInAs>>;
  beforeAll(async () => {
    await clearCommands();
    await deleteTestUsers();
    operator = await signedInAs('OPERATIONS');
    administrator = await signedInAs('ADMIN');
    reader = await signedInAs('USER');
  });
  beforeEach(async () => {
    await clearCommands();
    await withDb((client) =>
      client.query(
        "UPDATE users SET role='OPERATIONS', status='ACTIVE', deleted=false, session_version=0 WHERE id=$1",
        [operator.user.id]
      )
    );
    vi.restoreAllMocks();
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (url) => {
      if (String(url).endsWith('/state'))
        return Response.json({
          wellheadId: 1,
          modelVersion: '0.3.0',
          status: 'running',
        });
      if (String(url).endsWith('/parameter-changes/validate'))
        return Response.json({
          modelVersion: '0.3.0',
          runningParameterFingerprint: 'a'.repeat(64),
          proposedParameterFingerprint: 'b'.repeat(64),
          restartRequired: true,
        });
      if (String(url).endsWith('/controls'))
        return Response.json({ ...state, lastTickAt: new Date().toISOString() });
      return Response.json({ status: 'notification_accepted' }, { status: 202 });
    });
  });
  afterAll(async () => {
    vi.restoreAllMocks();
    await clearCommands();
    await deleteTestUsers();
  });

  async function draft(
    cookie = operator.cookie,
    key = randomUUID(),
    body: unknown = draftBody
  ) {
    return request('/twin/control-commands', {
      method: 'POST',
      cookie,
      headers: { 'Idempotency-Key': key },
      body,
    });
  }
  async function activate(id: string, cookie = operator.cookie) {
    return request(`/twin/control-commands/${id}/activate`, {
      method: 'POST',
      cookie,
      body: { confirm: true, expectedRevision: 0 },
    });
  }
  async function draftId() {
    const response = await draft();
    expect(response.status).toBe(201);
    return (await responseData<CommandResult>(response)).commandId;
  }

  it('keeps USER and ADMIN outside control authority', async () => {
    expect((await draft(reader.cookie)).status).toBe(403);
    expect((await draft(administrator.cookie)).status).toBe(403);
    expect(
      (await request('/twin/wellheads/1/controls', { cookie: reader.cookie })).status
    ).toBe(200);
  });
  it('rejects unknown, null, empty, out-of-range, and mixed shutdown targets', async () => {
    for (const targets of [
      { pressure: 20 },
      { pumpOn: null },
      {},
      { chokePositionPercent: 101 },
      { emergencyShutdown: false, pumpOn: true },
    ]) {
      expect(
        (await draft(operator.cookie, randomUUID(), { ...draftBody, targets })).status
      ).toBe(400);
    }
  });
  it('deduplicates a draft and refuses changed content under the same key', async () => {
    const key = randomUUID();
    const first = await draft(operator.cookie, key);
    const second = await draft(operator.cookie, key);
    expect(first.status).toBe(201);
    expect(second.status).toBe(201);
    expect((await responseData<CommandResult>(first)).commandId).toBe(
      (await responseData<CommandResult>(second)).commandId
    );
    expect(
      (await draft(operator.cookie, key, { ...draftBody, reason: 'different' })).status
    ).toBe(409);
  });
  it('queues once with an atomic audit and an empty wake-up hint', async () => {
    const id = await draftId();
    const response = await activate(id);
    expect(response.status).toBe(202);
    expect((await responseData<CommandResult>(response)).status).toBe('queued');
    expect((await activate(id)).status).toBe(202);
    await withDb(async (client) => {
      const event = await client.query(
        "SELECT count(*)::int AS n FROM control_command_event WHERE command_id=$1 AND event_type='queued'",
        [id]
      );
      expect(event.rows[0].n).toBe(1);
      const audit = await client.query(
        "SELECT count(*)::int AS n FROM auditevent WHERE subject_id=$1 AND action='control.queued'",
        [id]
      );
      expect(audit.rows[0].n).toBe(1);
      const active = await client.query(
        'SELECT revision FROM wellhead_control_state WHERE wellhead_id=1'
      );
      expect(active.rows[0].revision).toBe(0);
    });
    const hints = vi
      .mocked(fetch)
      .mock.calls.filter(([url]) => String(url).endsWith('/notify'));
    expect(hints.length).toBe(2);
    expect(hints[0]![1]!.body).toBe('{}');
  });
  it('allows cancellation and never requeues a terminal command', async () => {
    const id = await draftId();
    await activate(id);
    const cancelled = await request(`/twin/control-commands/${id}/cancel`, {
      method: 'POST',
      cookie: operator.cookie,
      body: { confirm: true },
    });
    expect(cancelled.status).toBe(200);
    expect((await responseData<CommandResult>(cancelled)).status).toBe('cancelled');
    expect((await responseData<CommandResult>(await activate(id))).status).toBe(
      'cancelled'
    );
  });
  it('checks the database role again even while the JWT still says OPERATIONS', async () => {
    const id = await draftId();
    await withDb((client) =>
      client.query("UPDATE users SET role='USER' WHERE id=$1", [operator.user.id])
    );
    expect((await activate(id)).status).toBe(403);
  });
  it('rejects disabled accounts and a revoked session', async () => {
    const id = await draftId();
    await withDb((client) =>
      client.query("UPDATE users SET status='DISABLED' WHERE id=$1", [operator.user.id])
    );
    expect((await activate(id)).status).toBe(403);
    await withDb((client) =>
      client.query("UPDATE users SET status='ACTIVE',session_version=1 WHERE id=$1", [
        operator.user.id,
      ])
    );
    expect((await activate(id)).status).toBe(401);
  });
  it('retains a committed queue entry when its HTTP hint fails', async () => {
    const id = await draftId();
    vi.mocked(fetch).mockImplementation(async (url) => {
      if (String(url).endsWith('/controls'))
        return Response.json({ ...state, lastTickAt: new Date().toISOString() });
      throw new Error('connection lost');
    });
    const response = await activate(id);
    expect(response.status).toBe(202);
    expect((await responseData<CommandResult>(response)).notificationPending).toBe(
      true
    );
  });
  it('serializes concurrent activations into one queued event', async () => {
    const id = await draftId();
    const responses = await Promise.all([activate(id), activate(id)]);
    expect(responses.map((r) => r.status)).toEqual([202, 202]);
    await withDb(async (client) => {
      const result = await client.query(
        "SELECT count(*)::int AS n FROM control_command_event WHERE command_id=$1 AND event_type='queued'",
        [id]
      );
      expect(result.rows[0].n).toBe(1);
    });
  });
  it('rolls back activation when its audit insert fails', async () => {
    const id = await draftId();
    await withDb((client) =>
      client.query(
        `CREATE FUNCTION test_refuse_control_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='control.queued' THEN RAISE EXCEPTION 'test audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_refuse_control_audit BEFORE INSERT ON auditevent FOR EACH ROW EXECUTE FUNCTION test_refuse_control_audit()`
      )
    );
    try {
      expect((await activate(id)).status).toBe(503);
      await withDb(async (client) => {
        const result = await client.query(
          'SELECT status FROM control_command WHERE command_id=$1',
          [id]
        );
        expect(result.rows[0].status).toBe('draft');
      });
    } finally {
      await withDb((client) =>
        client.query(
          'DROP TRIGGER test_refuse_control_audit ON auditevent; DROP FUNCTION test_refuse_control_audit()'
        )
      );
    }
  });
  it('uses a separate ADMIN proposal and reports publication requiring restart', async () => {
    const body = {
      wellheadId: 1,
      modelVersion: '0.3.0',
      parameterKey: 'downstream_pressure',
      value: 260,
      unit: 'psi',
      expectedOverrideId: null,
      reason: 'Reference well test',
    };
    const headers = { 'Idempotency-Key': randomUUID() };
    expect(
      (
        await request('/twin/parameter-changes', {
          method: 'POST',
          cookie: operator.cookie,
          headers,
          body,
        })
      ).status
    ).toBe(403);
    const proposed = await request('/twin/parameter-changes', {
      method: 'POST',
      cookie: administrator.cookie,
      headers,
      body,
    });
    expect(proposed.status).toBe(201);
    const id = (await responseData<ParameterResult>(proposed)).changeId;
    const publish = await request(`/twin/parameter-changes/${id}/publish`, {
      method: 'POST',
      cookie: administrator.cookie,
      body: { confirm: true },
    });
    expect(publish.status).toBe(200);
    const result = await responseData<ParameterResult>(publish);
    expect(result.status).toBe('published');
    expect(result.restartRequired).toBe(true);
    const stateResponse = await request('/twin/wellheads/1/parameters', {
      cookie: administrator.cookie,
    });
    expect(stateResponse.status).toBe(200);
    const parameters = await responseData<{
      latestOverrides: { parameterOverrideId: number }[];
    }>(stateResponse);
    expect(parameters.latestOverrides[0]?.parameterOverrideId).toBe(
      result.publishedOverrideId
    );
    await withDb(async (client) => {
      const row = await client.query(
        'SELECT changed_by,value FROM wellhead_model_parameter WHERE parameter_override_id=$1',
        [result.publishedOverrideId]
      );
      expect(row.rows[0]).toEqual({
        changed_by: String(administrator.user.id),
        value: 260,
      });
    });
  });
});
