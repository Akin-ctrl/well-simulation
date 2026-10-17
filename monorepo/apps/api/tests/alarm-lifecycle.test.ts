import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { db, sql } from '@well-simulation/db/query';
import type {
  AlarmList,
  AlarmReceipt,
  AlarmShelfList,
} from '@well-simulation/dto/res/alarms';
import { deleteTestUsers, request, signedInAs, withDb } from './helpers/api';
import { resetRateLimits } from '../src/middleware/rate-limiter';

async function data<T>(response: Response): Promise<T> {
  return ((await response.json()) as { data: T }).data;
}
const eventId = '9007199254740993';
const triggeredAt = new Date().toISOString().replace(/\.[0-9]{3}Z$/, '.123456Z');
const common = {
  reason: 'Inspecting the instrument and current readings',
  confirmed: true,
};

describe('Alarm operator lifecycle', () => {
  let operator: Awaited<ReturnType<typeof signedInAs>>,
    other: typeof operator,
    reader: typeof operator,
    admin: typeof operator;
  let well: number, device: number, rule: number;
  const write = (body: unknown, key = randomUUID(), cookie = operator.cookie) =>
    request('/alarms/actions', {
      method: 'POST',
      cookie,
      body,
      headers: { 'Idempotency-Key': key },
    });
  const ack = () => ({ action: 'acknowledge', eventId, triggeredAt, ...common });
  const shelve = (revision = 0) => ({
    action: 'shelve',
    wellheadId: well,
    ruleId: rule,
    expectedRevision: revision,
    durationMinutes: 30,
    ...common,
  });
  const unshelve = (revision = 1) => ({
    action: 'unshelve',
    wellheadId: well,
    ruleId: rule,
    expectedRevision: revision,
    ...common,
  });
  const list = async (view = 'attention', cursor?: string) => {
    const response = await request(
      `/alarms?view=${view}&wellheadId=${well}${cursor ? '&cursor=' + encodeURIComponent(cursor) : ''}`,
      { cookie: reader.cookie }
    );
    expect(response.status).toBe(200);
    return data<AlarmList>(response);
  };
  const shelves = async () => {
    const response = await request(`/alarms/shelves?wellheadId=${well}`, {
      cookie: reader.cookie,
    });
    expect(response.status).toBe(200);
    return data<AlarmShelfList>(response);
  };
  async function clear() {
    await withDb((client) =>
      client.query(
        `DELETE FROM alarm_acknowledgement WHERE action_id IN (SELECT action_id FROM alarm_action WHERE wellhead_id=$1);
  `.trim(),
        [well]
      )
    );
    await withDb(async (client) => {
      await client.query('DELETE FROM alarm_shelf WHERE wellhead_id=$1', [well]);
      await client.query('DELETE FROM alarm_action WHERE wellhead_id=$1', [well]);
      await client.query('DELETE FROM alarmevent WHERE wellhead_id=$1', [well]);
    });
  }
  beforeAll(async () => {
    await deleteTestUsers();
    operator = await signedInAs('OPERATIONS');
    other = await signedInAs('OPERATIONS');
    reader = await signedInAs('USER');
    admin = await signedInAs('ADMIN');
    await withDb(async (client) => {
      device = (
        await client.query(
          "INSERT INTO device(name,modbus_unit_id) VALUES ('alarm-lifecycle-test',247) RETURNING device_id"
        )
      ).rows[0].device_id;
      well = (
        await client.query(
          "INSERT INTO wellhead(location_id,device_id,name) VALUES (1,$1,'Alarm lifecycle test well') RETURNING wellhead_id",
          [device]
        )
      ).rows[0].wellhead_id;
      rule = (
        await client.query(
          "SELECT alarm_rule_id FROM alarmrule WHERE severity_level='CRITICAL' ORDER BY alarm_rule_id LIMIT 1"
        )
      ).rows[0].alarm_rule_id;
    });
  });
  beforeEach(async () => {
    await clear();
    await withDb(async (client) => {
      await client.query(
        "UPDATE users SET role='OPERATIONS',status='ACTIVE',deleted=false,session_version=0 WHERE id=$1",
        [operator.user.id]
      );
      await client.query(
        "INSERT INTO alarmevent(event_id,alarm_rule_id,parameter_reading_id,timestamp_utc,wellhead_id,triggered_at,severity_level,triggered_value) VALUES($1,$2,1,$3,$4,$3,'CRITICAL',3200)",
        [eventId, rule, triggeredAt, well]
      );
    });
  });
  afterAll(async () => {
    await clear();
    await withDb(async (client) => {
      await client.query('DELETE FROM wellhead WHERE wellhead_id=$1', [well]);
      await client.query('DELETE FROM device WHERE device_id=$1', [device]);
    });
    await deleteTestUsers();
  });
  it('keeps the real application role unable to alter alarm truth or receipts', async () => {
    const result = await db.execute(sql`SELECT current_user AS actor,
    has_table_privilege(current_user,'alarmevent','UPDATE') AS events,
    has_table_privilege(current_user,'alarm_action','UPDATE') AS receipts,
    has_table_privilege(current_user,'alarm_acknowledgement','DELETE') AS acknowledgements,
    has_column_privilege(current_user,'alarm_shelf','revision','UPDATE') AS shelves`);
    expect(result.rows[0]).toMatchObject({
      actor: process.env.TEST_API_DB_USER ?? process.env.API_DB_USER,
      events: false,
      receipts: false,
      acknowledgements: false,
      shelves: true,
    });
  });
  it('allows inspection but reserves all writes for OPERATIONS', async () => {
    expect((await request('/alarms')).status).toBe(401);
    for (const body of [ack(), shelve(), unshelve()])
      for (const cookie of [reader.cookie, admin.cookie])
        expect((await write(body, randomUUID(), cookie)).status).toBe(403);
    expect((await list()).items[0]?.eventId).toBe(eventId);
  });
  it('acknowledges the exact bigint and microsecond event without clearing it', async () => {
    await withDb((client) =>
      client.query(
        "INSERT INTO alarmevent SELECT event_id,alarm_rule_id,parameter_reading_id,timestamp_utc,wellhead_id,triggered_at+interval '1 microsecond',cleared_at,severity_level,triggered_value FROM alarmevent WHERE wellhead_id=$1",
        [well]
      )
    );
    const response = await write(ack());
    expect(response.status).toBe(200);
    const result = await data<AlarmReceipt>(response);
    expect(result.eventId).toBe(eventId);
    expect(result.triggeredAt).toBe(triggeredAt);
    const state = await list('active');
    expect(state.summary.active).toBe(2);
    expect(state.summary.attention).toBe(1);
    expect(state.items[0]?.acknowledgement?.actorName).toBe(operator.user.username);
    expect(state.items[1]?.acknowledgement).toBeNull();
  });
  it('retains a returned-to-normal event in attention until acknowledged', async () => {
    await withDb((client) =>
      client.query(
        'UPDATE alarmevent SET cleared_at=clock_timestamp() WHERE wellhead_id=$1',
        [well]
      )
    );
    expect((await list()).summary).toMatchObject({ active: 0, attention: 1 });
    expect((await write(ack())).status).toBe(200);
    expect((await list()).summary.attention).toBe(0);
    expect((await list('history')).items).toHaveLength(1);
  });
  it('replays the same request and rejects a changed payload with that key', async () => {
    const key = randomUUID();
    const first = await data<AlarmReceipt>(await write(ack(), key));
    const replay = await write(ack(), key);
    expect(replay.status).toBe(200);
    expect(await data(replay)).toEqual(first);
    expect(
      (await write({ ...ack(), reason: 'A different action reason' }, key)).status
    ).toBe(409);
    expect(
      (await request('/alarms/actions/' + key, { cookie: other.cookie })).status
    ).toBe(404);
    expect(
      await data(await request('/alarms/actions/' + key, { cookie: operator.cookie }))
    ).toEqual(first);
    await withDb(async (client) => {
      expect(
        (
          await client.query(
            'SELECT count(*)::int AS count FROM alarm_action WHERE wellhead_id=$1',
            [well]
          )
        ).rows[0].count
      ).toBe(1);
      expect(
        (
          await client.query(
            'SELECT count(*)::int AS count FROM auditevent WHERE subject_id=$1',
            [first.actionId]
          )
        ).rows[0].count
      ).toBe(1);
    });
  });
  it('accepts equivalent UTC spellings for an idempotent acknowledgement', async () => {
    const key = randomUUID();
    expect((await write(ack(), key)).status).toBe(200);
    expect(
      (await write({ ...ack(), triggeredAt: triggeredAt.replace('Z', '+00:00') }, key))
        .status
    ).toBe(200);
  });
  it('allows only one competing acknowledgement', async () => {
    const responses = await Promise.all([
      write(ack()),
      write(ack(), randomUUID(), other.cookie),
    ]);
    expect(responses.map((x) => x.status).sort()).toEqual([200, 409]);
  });
  it('keeps active truth and counts while a shelf survives clear and retrigger', async () => {
    const result = await data<AlarmReceipt>(await write(shelve()));
    expect(Date.parse(result.shelvedUntil!) - Date.parse(result.occurredAt)).toBe(
      30 * 60000
    );
    expect((await list()).summary).toMatchObject({
      active: 1,
      attention: 0,
      shelves: 1,
    });
    await withDb(async (client) => {
      await client.query(
        'UPDATE alarmevent SET cleared_at=clock_timestamp() WHERE wellhead_id=$1',
        [well]
      );
    });
    expect((await shelves()).items).toHaveLength(1);
    expect((await list('active')).items).toHaveLength(0);
    await withDb((client) =>
      client.query(
        "INSERT INTO alarmevent(event_id,alarm_rule_id,parameter_reading_id,timestamp_utc,wellhead_id,triggered_at,severity_level,triggered_value) VALUES($1,$2,1,clock_timestamp(),$3,clock_timestamp(),'CRITICAL',3201)",
        [(BigInt(eventId) + 1n).toString(), rule, well]
      )
    );
    expect((await list()).summary).toMatchObject({
      active: 1,
      attention: 0,
      shelves: 1,
    });
    expect((await write(unshelve())).status).toBe(200);
    expect((await list()).summary).toMatchObject({
      active: 1,
      attention: 2,
      shelves: 0,
    });
  });
  it('expires shelves by database time without a cleanup worker', async () => {
    expect((await write(shelve())).status).toBe(200);
    await withDb((client) =>
      client.query(
        "UPDATE alarm_shelf SET updated_at=statement_timestamp()-interval '6 minutes',shelved_until=statement_timestamp()-interval '1 minute' WHERE wellhead_id=$1",
        [well]
      )
    );
    expect((await shelves()).items).toHaveLength(0);
    expect((await list()).summary.attention).toBe(1);
    expect((await write(unshelve())).status).toBe(409);
    expect((await write(shelve(1))).status).toBe(200);
  });
  it('rejects stale shelf revisions and serializes the absent-row race', async () => {
    const results = await Promise.all([
      write(shelve()),
      write(shelve(), randomUUID(), other.cookie),
    ]);
    expect(results.map((x) => x.status).sort()).toEqual([200, 409]);
    expect((await write(shelve(0))).status).toBe(409);
    expect((await write(unshelve(0))).status).toBe(409);
    const race = await Promise.all([
      write(shelve(1)),
      write(unshelve(1), randomUUID(), other.cookie),
    ]);
    expect(race.map((x) => x.status).sort()).toEqual([200, 409]);
  });
  it('rolls back receipt and state if auditing fails', async () => {
    await withDb((client) =>
      client.query(
        "CREATE FUNCTION test_refuse_alarm_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action LIKE 'alarm.%' THEN RAISE EXCEPTION 'test audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_refuse_alarm_audit BEFORE INSERT ON auditevent FOR EACH ROW EXECUTE FUNCTION test_refuse_alarm_audit()"
      )
    );
    try {
      for (const body of [ack(), shelve()])
        expect((await write(body)).status).toBe(503);
      expect((await list()).summary).toMatchObject({ attention: 1, shelves: 0 });
      await withDb(async (client) =>
        expect(
          (
            await client.query(
              'SELECT count(*)::int AS count FROM alarm_action WHERE wellhead_id=$1',
              [well]
            )
          ).rows[0].count
        ).toBe(0)
      );
    } finally {
      await withDb((client) =>
        client.query(
          'DROP TRIGGER test_refuse_alarm_audit ON auditevent; DROP FUNCTION test_refuse_alarm_audit()'
        )
      );
    }
  });
  it('rechecks current role and revoked sessions', async () => {
    await withDb((client) =>
      client.query("UPDATE users SET role='USER' WHERE id=$1", [operator.user.id])
    );
    expect((await write(ack())).status).toBe(403);
    await withDb((client) =>
      client.query(
        "UPDATE users SET role='OPERATIONS',session_version=session_version+1 WHERE id=$1",
        [operator.user.id]
      )
    );
    expect((await write(ack())).status).toBe(401);
  });
  it('validates duration, confirmation, reason, exact identity and unexpected fields', async () => {
    for (const body of [
      { ...shelve(), durationMinutes: 4 },
      { ...shelve(), durationMinutes: 481 },
      { ...shelve(), durationMinutes: 5.5 },
      { ...shelve(), expectedRevision: -1 },
      { ...ack(), confirmed: false },
      { ...ack(), reason: ' ' },
      { ...ack(), eventId: Number(eventId) },
      { ...ack(), triggeredAt: triggeredAt.replace('123456', '1234567') },
      { ...ack(), actorUserId: other.user.id },
    ]) {
      resetRateLimits();
      expect((await write(body)).status).toBe(400);
    }
    resetRateLimits();
    expect((await write({ ...ack(), reason: 'x'.repeat(5000) })).status).toBe(413);
    expect(
      (
        await request('/alarms/actions', {
          method: 'POST',
          cookie: operator.cookie,
          body: ack(),
          headers: { 'Idempotency-Key': randomUUID() },
          origin: 'https://untrusted.example',
        })
      ).status
    ).toBe(403);
  });
  it('bounds page reads and preserves identities across pages', async () => {
    await withDb((client) =>
      client.query(
        "INSERT INTO alarmevent(event_id,alarm_rule_id,parameter_reading_id,timestamp_utc,wellhead_id,triggered_at,severity_level,triggered_value) SELECT $1::bigint+n,$2,1,$3::timestamptz,$4,$3::timestamptz+n*interval '1 microsecond','WARNING',3000 FROM generate_series(1,54) n",
        [eventId, rule, triggeredAt, well]
      )
    );
    const first = await list();
    expect(first.items).toHaveLength(50);
    expect(first.summary.attention).toBe(55);
    expect(first.nextCursor).not.toBeNull();
    const second = await list('attention', first.nextCursor!);
    expect(second.items).toHaveLength(5);
    expect(second.nextCursor).toBeNull();
    const newest = await list('history');
    const older = await list('history', newest.nextCursor!);
    expect(newest.items[0]?.eventId).toBe((BigInt(eventId) + 54n).toString());
    expect(older.items.at(-1)?.eventId).toBe(eventId);
    expect(
      new Set([...newest.items, ...older.items].map((item) => item.eventId)).size
    ).toBe(55);
    expect(
      new Set(
        [...first.items, ...second.items].map((x) => x.eventId + ':' + x.triggeredAt)
      ).size
    ).toBe(55);
    expect(
      (
        await request(
          '/alarms?view=history&wellheadId=' + well + '&cursor=' + first.nextCursor,
          { cookie: reader.cookie }
        )
      ).status
    ).toBe(400);
    expect(
      (await request('/alarms?cursor=broken', { cookie: reader.cookie })).status
    ).toBe(400);
  });
  it('rolls back edits to an existing shelf when its audit insert fails', async () => {
    expect((await write(shelve())).status).toBe(200);
    const before = (await shelves()).items[0];
    await withDb((client) =>
      client.query(
        "CREATE FUNCTION test_refuse_alarm_edit_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action LIKE 'alarm.%' THEN RAISE EXCEPTION 'test audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_refuse_alarm_edit_audit BEFORE INSERT ON auditevent FOR EACH ROW EXECUTE FUNCTION test_refuse_alarm_edit_audit()"
      )
    );
    try {
      for (const body of [shelve(1), unshelve(1)])
        expect((await write(body)).status).toBe(503);
      expect((await shelves()).items[0]).toEqual(before);
      await withDb(async (client) =>
        expect(
          (
            await client.query(
              'SELECT count(*)::int AS count FROM alarm_action WHERE wellhead_id=$1',
              [well]
            )
          ).rows[0].count
        ).toBe(1)
      );
    } finally {
      await withDb((client) =>
        client.query(
          'DROP TRIGGER test_refuse_alarm_edit_audit ON auditevent; DROP FUNCTION test_refuse_alarm_edit_audit()'
        )
      );
    }
  });
  it('paginates current shelves independently of retained events', async () => {
    const rules: number[] = [];
    try {
      await withDb(async (client) => {
        const result = await client.query(
          'INSERT INTO alarmrule(parameter_type_id,severity_level,operator,threshold_value,active) SELECT parameter_type_id, severity_level, operator, 900000+n, false FROM alarmrule CROSS JOIN generate_series(1,55) n WHERE alarm_rule_id=$1 RETURNING alarm_rule_id',
          [rule]
        );
        rules.push(...result.rows.map((row) => row.alarm_rule_id));
        for (const id of rules) {
          const actionId = randomUUID();
          await client.query(
            "INSERT INTO alarm_action(action_id,actor_user_id,actor_name,actor_role,request_key,input_hash,action,wellhead_id,alarm_rule_id,occurred_at,input,result) VALUES($1,$2,'Page fixture','OPERATIONS',$3,$4,'shelve',$5,$6,statement_timestamp(),'{\"reason\":\"Page fixture\"}','{}')",
            [actionId, operator.user.id, randomUUID(), 'a'.repeat(64), well, id]
          );
          await client.query(
            "INSERT INTO alarm_shelf(wellhead_id,alarm_rule_id,revision,shelved_until,updated_at,action_id) VALUES($1,$2,1,statement_timestamp()+interval '30 minutes',statement_timestamp(),$3)",
            [well, id, actionId]
          );
        }
      });
      const first = await shelves();
      expect(first.total).toBe(55);
      expect(first.items).toHaveLength(50);
      const response = await request(
        '/alarms/shelves?wellheadId=' + well + '&cursor=' + first.nextCursor,
        { cookie: reader.cookie }
      );
      expect(response.status).toBe(200);
      const second = await data<AlarmShelfList>(response);
      expect(second.items).toHaveLength(5);
      expect(second.nextCursor).toBeNull();
      expect(
        new Set([...first.items, ...second.items].map((item) => item.target.ruleId))
          .size
      ).toBe(55);
      expect((await list()).summary.active).toBe(1);
    } finally {
      await clear();
      await withDb((client) =>
        client.query('DELETE FROM alarmrule WHERE alarm_rule_id=ANY($1::int[])', [
          rules,
        ])
      );
    }
  });
  it('keeps receipts after event retention and exposes bounded action history', async () => {
    const key = randomUUID();
    expect((await write(ack(), key)).status).toBe(200);
    const detail = await request(
      '/alarms/events/' + eventId + '?triggeredAt=' + encodeURIComponent(triggeredAt),
      { cookie: reader.cookie }
    );
    expect(detail.status).toBe(200);
    expect((await data<{ actions: unknown[] }>(detail)).actions).toHaveLength(1);
    await withDb((client) =>
      client.query('DELETE FROM alarmevent WHERE wellhead_id=$1', [well])
    );
    expect(
      (await request('/alarms/actions/' + key, { cookie: operator.cookie })).status
    ).toBe(200);
    expect(
      (
        await request(
          '/alarms/events/' +
            eventId +
            '?triggeredAt=' +
            encodeURIComponent(triggeredAt),
          { cookie: reader.cookie }
        )
      ).status
    ).toBe(404);
  });
});
