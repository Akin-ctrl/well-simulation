import { db, sql } from '@well-simulation/db/query';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { deleteTestUsers, request, signedInAs, withDb } from './helpers/api';
import {
  forecastScheduleSchema,
  type ForecastScheduleView,
} from '@well-simulation/dto/res/forecast-schedule';
async function data<T>(response: Response): Promise<T> {
  return ((await response.json()) as { data: T }).data;
}
const path = '/twin/wellheads/1/forecast-schedule';
const draft = {
  expectedRevision: 0,
  enabled: false,
  intervalMinutes: 15,
  durationMinutes: 15,
  reason: 'Keep a recent result for this well',
  confirmed: true,
};

describe('Scheduled forecast configuration', () => {
  let admin: Awaited<ReturnType<typeof signedInAs>>;
  let reader: Awaited<ReturnType<typeof signedInAs>>;
  let otherAdmin: Awaited<ReturnType<typeof signedInAs>>;
  let operator: Awaited<ReturnType<typeof signedInAs>>;
  beforeAll(async () => {
    await deleteTestUsers();
    admin = await signedInAs('ADMIN');
    reader = await signedInAs('USER');
    operator = await signedInAs('OPERATIONS');
    otherAdmin = await signedInAs('ADMIN');
  });
  beforeEach(async () => {
    await withDb(async (db) => {
      await db.query('DELETE FROM forecast_job');
      await db.query('DELETE FROM forecast_schedule');
      await db.query("DELETE FROM auditevent WHERE action='forecast_schedule.updated'");
      await db.query("UPDATE users SET role='ADMIN', status='ACTIVE' WHERE id=$1", [
        admin.user.id,
      ]);
    });
  });
  afterAll(async () => {
    await withDb(async (db) => {
      await db.query('DELETE FROM forecast_job');
      await db.query('DELETE FROM forecast_schedule');
    });
    await deleteTestUsers();
  });
  it('uses application permissions rather than a database administrator', async () => {
    const result = await db.execute(
      sql`SELECT current_user AS actor, has_table_privilege(current_user, 'wellhead', 'UPDATE') AS may_update_assets, has_table_privilege(current_user, 'forecast_schedule', 'UPDATE') AS may_update_schedules`
    );
    expect(result.rows[0]).toMatchObject({
      actor: process.env.TEST_API_DB_USER ?? process.env.API_DB_USER,
      may_update_assets: false,
      may_update_schedules: true,
    });
  });
  it('lets readers inspect empty schedules but reserves writes for administrators', async () => {
    expect((await request(path)).status).toBe(401);
    const read = await request(path, { cookie: reader.cookie });
    expect(read.status).toBe(200);
    expect(await data<ForecastScheduleView>(read)).toEqual({
      schedule: null,
      latestJob: null,
      latestCompleted: null,
    });
    for (const cookie of [reader.cookie, operator.cookie])
      expect((await request(path, { method: 'PUT', cookie, body: draft })).status).toBe(
        403
      );
    expect(
      (await request(path, { method: 'PUT', cookie: admin.cookie, body: draft })).status
    ).toBe(200);
  });
  it('validates bounds, explicit confirmation, identifiers and unknown inputs', async () => {
    for (const change of [
      { intervalMinutes: 4 },
      { intervalMinutes: 1441 },
      { intervalMinutes: 5.5 },
      { durationMinutes: 31 },
      { durationMinutes: 4 },
      { reason: ' ' },
      { confirmed: false },
      { expectedRevision: -1 },
      { ownerUserId: 1 },
    ]) {
      expect(
        (
          await request(path, {
            method: 'PUT',
            cookie: admin.cookie,
            body: { ...draft, ...change },
          })
        ).status
      ).toBe(400);
    }
    expect(
      (
        await request('/twin/wellheads/2147483648/forecast-schedule', {
          cookie: reader.cookie,
        })
      ).status
    ).toBe(400);
    expect(
      (
        await request('/twin/wellheads/2147483647/forecast-schedule', {
          cookie: reader.cookie,
        })
      ).status
    ).toBe(404);
  });
  it('records attributed edits, rejects old revisions and starts a new interval', async () => {
    const first = await request(path, {
      method: 'PUT',
      cookie: admin.cookie,
      body: draft,
    });
    const initial = forecastScheduleSchema.parse(await data<unknown>(first));
    expect(initial).toMatchObject({
      enabled: false,
      nextDueAt: null,
      ownerUserId: admin.user.id,
      revision: 1,
    });
    expect(
      (await request(path, { method: 'PUT', cookie: admin.cookie, body: draft })).status
    ).toBe(409);
    const second = await request(path, {
      method: 'PUT',
      cookie: admin.cookie,
      body: { ...draft, expectedRevision: 1, enabled: true, intervalMinutes: 5 },
    });
    const updated = forecastScheduleSchema.parse(await data<unknown>(second));
    expect(updated.revision).toBe(2);
    expect(
      Date.parse(updated.nextDueAt ?? 'invalid') - Date.parse(updated.updatedAt)
    ).toBeCloseTo(300_000, -1);
    await withDb(async (db) => {
      const audit = await db.query(
        "SELECT actor_user_id, detail FROM auditevent WHERE action='forecast_schedule.updated' ORDER BY occurred_at DESC"
      );
      expect(audit.rows).toHaveLength(2);
      expect(audit.rows[0]).toMatchObject({
        actor_user_id: admin.user.id,
        detail: { revision: 2, reason: draft.reason },
      });
    });
  });
  it('rejects competing owners without overwriting the winning create', async () => {
    const responses = await Promise.all([
      request(path, { method: 'PUT', cookie: admin.cookie, body: draft }),
      request(path, {
        method: 'PUT',
        cookie: otherAdmin.cookie,
        body: { ...draft, intervalMinutes: 5 },
      }),
    ]);
    expect(responses.map((response) => response.status).sort()).toEqual([200, 409]);
    await withDb(async (db) =>
      expect((await db.query('SELECT revision FROM forecast_schedule')).rows).toEqual([
        { revision: 1 },
      ])
    );
  });
  it('rechecks current authority even while a signed token still says ADMIN', async () => {
    await withDb((db) =>
      db.query("UPDATE users SET role='USER' WHERE id=$1", [admin.user.id])
    );
    expect(
      (await request(path, { method: 'PUT', cookie: admin.cookie, body: draft })).status
    ).toBe(403);
    await withDb((db) =>
      db.query("UPDATE users SET role='ADMIN', status='DISABLED' WHERE id=$1", [
        admin.user.id,
      ])
    );
    expect([401, 403]).toContain(
      (await request(path, { method: 'PUT', cookie: admin.cookie, body: draft })).status
    );
  });
  it('rolls back settings when their audit record fails', async () => {
    await withDb((db) =>
      db.query(
        "CREATE FUNCTION test_refuse_forecast_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='forecast_schedule.updated' THEN RAISE EXCEPTION 'test audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_refuse_forecast_audit BEFORE INSERT ON auditevent FOR EACH ROW EXECUTE FUNCTION test_refuse_forecast_audit()"
      )
    );
    try {
      expect(
        (await request(path, { method: 'PUT', cookie: admin.cookie, body: draft }))
          .status
      ).toBe(503);
      const response = await request(path, { cookie: admin.cookie });
      const view: ForecastScheduleView = await data<ForecastScheduleView>(response);
      expect(view.schedule).toBeNull();
    } finally {
      await withDb((db) =>
        db.query(
          'DROP TRIGGER test_refuse_forecast_audit ON auditevent; DROP FUNCTION test_refuse_forecast_audit()'
        )
      );
    }
  });
});
