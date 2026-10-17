import { HTTPException } from 'hono/http-exception';
import { db, sql, type SQL } from '@well-simulation/db/query';
import { ALARM_SEVERITIES } from '@well-simulation/dto/res/dashboard';
import {
  ALARM_PAGE_SIZE,
  eventCursorSchema,
  shelfCursorSchema,
  type AlarmQuery,
  type ShelfQuery,
} from '@well-simulation/dto/req/alarms';
import {
  alarmDetailSchema,
  alarmEventSchema,
  alarmListSchema,
  alarmReceiptSchema,
  alarmShelfListSchema,
  alarmTargetSchema,
} from '@well-simulation/dto/res/alarms';
import type { Transaction } from './transaction-authority';

/** Convert in SQL before pg can reduce a timestamp to JavaScript milliseconds. */
export const exactTime = (value: SQL) =>
  sql`to_char((${value}) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
export const targetJson = sql`jsonb_build_object(
  'wellheadId', w.wellhead_id, 'wellheadName', w.name, 'locationName', l.name,
  'ruleId', r.alarm_rule_id, 'parameterName', p.display_name, 'unit', p.canonical_unit,
  'operator', r.operator, 'threshold', r.threshold_value, 'severity', r.severity_level,
  'triggerAfter', r.trigger_after, 'clearAfter', r.clear_after)`;
const eventFrom = sql`FROM alarmevent e
  JOIN wellhead w ON w.wellhead_id=e.wellhead_id
  JOIN location l ON l.location_id=w.location_id
  JOIN alarmrule r ON r.alarm_rule_id=e.alarm_rule_id
  JOIN parametertype p ON p.parameter_type_id=r.parameter_type_id
  LEFT JOIN alarm_acknowledgement a ON a.event_id=e.event_id AND a.triggered_at=e.triggered_at
  LEFT JOIN alarm_action aa ON aa.action_id=a.action_id
  LEFT JOIN alarm_shelf s ON s.wellhead_id=e.wellhead_id AND s.alarm_rule_id=e.alarm_rule_id
  LEFT JOIN alarm_action sa ON sa.action_id=s.action_id`;
const shelfFrom = sql`FROM alarm_shelf s
  JOIN wellhead w ON w.wellhead_id=s.wellhead_id JOIN location l ON l.location_id=w.location_id
  JOIN alarmrule r ON r.alarm_rule_id=s.alarm_rule_id
  JOIN parametertype p ON p.parameter_type_id=r.parameter_type_id
  JOIN alarm_action sa ON sa.action_id=s.action_id`;
const priority = sql`CASE e.severity_level ${sql.join(
  ALARM_SEVERITIES.map((severity, rank) => sql`WHEN ${severity} THEN ${rank}::int`),
  sql` `
)} ELSE ${ALARM_SEVERITIES.length}::int END`;
const shelfJson = (at: string) => sql`jsonb_build_object('revision',s.revision,
  'until',${exactTime(sql`s.shelved_until`)}, 'active',coalesce(s.shelved_until > ${at}::timestamptz,false),
  'updatedAt',${exactTime(sql`s.updated_at`)},'actorName',sa.actor_name,'reason',sa.input->>'reason')`;
const eventJson = (at: string) => sql`jsonb_build_object(
  'eventId',e.event_id::text,'triggeredAt',${exactTime(sql`e.triggered_at`)},
  'clearedAt',${exactTime(sql`e.cleared_at`)},'severity',e.severity_level,'triggeredValue',e.triggered_value,
  'target',${targetJson}, 'acknowledgement',CASE WHEN a.action_id IS NULL THEN NULL ELSE
    jsonb_build_object('at',${exactTime(sql`a.acknowledged_at`)},'actorName',aa.actor_name,'reason',aa.input->>'reason') END,
  'shelf',CASE WHEN s.revision IS NULL THEN NULL ELSE ${shelfJson(at)} END)`;

/** Counts and rows share one snapshot and one database observation time. */
async function readSnapshot<T>(
  read: (tx: Transaction, at: string) => Promise<T>
): Promise<T> {
  return db.transaction(
    async (tx) => {
      await tx.execute(sql`SET LOCAL statement_timeout = '3000ms'`);
      const clock = await tx.execute<{ at: string }>(
        sql`SELECT ${exactTime(sql`transaction_timestamp()`)} AS at`
      );
      return read(tx, clock.rows[0]!.at);
    },
    { isolationLevel: 'repeatable read', accessMode: 'read only' }
  );
}
function decode<T>(
  value: string | undefined,
  schema: { parse(value: unknown): T }
): T | null {
  if (!value) return null;
  try {
    if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new Error('encoding');
    return schema.parse(JSON.parse(Buffer.from(value, 'base64url').toString('utf8')));
  } catch {
    throw new HTTPException(400, { message: 'Invalid alarm page cursor' });
  }
}
const encode = (value: unknown) =>
  Buffer.from(JSON.stringify(value)).toString('base64url');
const wellFilter = (wellheadId: number | undefined) =>
  wellheadId ? sql`w.wellhead_id=${wellheadId}` : sql`TRUE`;

async function pageSummary(tx: Transaction, at: string, wellheadId?: number) {
  const attention = sql`a.action_id IS NULL AND (s.shelved_until IS NULL OR s.shelved_until <= ${at}::timestamptz)`;
  const totals = await tx.execute(sql`SELECT count(*)::int AS retained,
      count(*) FILTER (WHERE e.cleared_at IS NULL)::int AS active,
      count(*) FILTER (WHERE e.cleared_at IS NULL AND e.severity_level='CRITICAL')::int AS critical,
      count(*) FILTER (WHERE ${attention})::int AS attention ${eventFrom} WHERE ${wellFilter(wellheadId)}`);
  const shelves = await tx.execute<{ count: number }>(
    sql`SELECT count(*)::int AS count ${shelfFrom} WHERE ${wellFilter(wellheadId)} AND s.shelved_until > ${at}::timestamptz`
  );
  const wells = await tx.execute(
    sql`SELECT wellhead_id AS "wellheadId",name AS "wellheadName" FROM wellhead ORDER BY name,wellhead_id`
  );
  return {
    summary: { ...totals.rows[0], shelves: shelves.rows[0]!.count },
    wells: wells.rows,
  };
}

export async function listAlarms(query: AlarmQuery) {
  const cursor = decode(query.cursor, eventCursorSchema);
  if (
    cursor &&
    (cursor.view !== query.view || cursor.wellheadId !== (query.wellheadId ?? null))
  )
    throw new HTTPException(400, { message: 'Cursor belongs to another alarm view' });
  return readSnapshot(async (tx, at) => {
    const attention = sql`a.action_id IS NULL AND (s.shelved_until IS NULL OR s.shelved_until <= ${at}::timestamptz)`;
    const header = await pageSummary(tx, at, query.wellheadId);
    const filter =
      query.view === 'attention'
        ? attention
        : query.view === 'active'
          ? sql`e.cleared_at IS NULL`
          : sql`TRUE`;
    const after = !cursor
      ? sql`TRUE`
      : query.view === 'history'
        ? sql`(e.triggered_at,e.event_id) < (${cursor.at}::timestamptz,${cursor.id}::bigint)`
        : sql`(${priority},e.triggered_at,e.event_id) > (${cursor.rank},${cursor.at}::timestamptz,${cursor.id}::bigint)`;
    const order =
      query.view === 'history'
        ? sql`e.triggered_at DESC,e.event_id DESC`
        : sql`${priority},e.triggered_at,e.event_id`;
    const result = await tx.execute(sql`SELECT ${eventJson(at)} AS item ${eventFrom}
      WHERE ${wellFilter(query.wellheadId)} AND ${filter} AND ${after} ORDER BY ${order} LIMIT ${ALARM_PAGE_SIZE + 1}`);
    const items = result.rows
      .slice(0, ALARM_PAGE_SIZE)
      .map((row) => alarmEventSchema.parse(row.item));
    const last = items.at(-1);
    const rank = last
      ? ALARM_SEVERITIES.findIndex((value) => value === last.severity)
      : -1;
    const nextCursor =
      result.rows.length > ALARM_PAGE_SIZE && last
        ? encode({
            wellheadId: query.wellheadId ?? null,
            view: query.view,
            rank: rank < 0 ? ALARM_SEVERITIES.length : rank,
            at: last.triggeredAt,
            id: last.eventId,
          })
        : null;
    return alarmListSchema.parse({
      observedAt: at,
      ...header,
      items,
      nextCursor,
    });
  });
}

export async function listShelves(query: ShelfQuery) {
  const cursor = decode(query.cursor, shelfCursorSchema);
  if (cursor && cursor.wellheadId !== (query.wellheadId ?? null))
    throw new HTTPException(400, { message: 'Cursor belongs to another well filter' });
  return readSnapshot(async (tx, at) => {
    const header = await pageSummary(tx, at, query.wellheadId);
    const condition = sql`${wellFilter(query.wellheadId)} AND s.shelved_until > ${at}::timestamptz`;
    const count = await tx.execute<{ count: number }>(
      sql`SELECT count(*)::int AS count ${shelfFrom} WHERE ${condition}`
    );
    const after = cursor
      ? sql`(s.wellhead_id,s.alarm_rule_id)>(${cursor.well},${cursor.rule})`
      : sql`TRUE`;
    const rows = await tx.execute(
      sql`SELECT ${targetJson} AS target, ${shelfJson(at)} AS state ${shelfFrom} WHERE ${condition} AND ${after} ORDER BY s.wellhead_id,s.alarm_rule_id LIMIT ${ALARM_PAGE_SIZE + 1}`
    );
    const items = rows.rows.slice(0, ALARM_PAGE_SIZE);
    const last = items.at(-1);
    const target = last ? alarmTargetSchema.parse(last.target) : null;
    return alarmShelfListSchema.parse({
      ...header,
      observedAt: at,
      items,
      total: count.rows[0]!.count,
      nextCursor:
        rows.rows.length > ALARM_PAGE_SIZE && target
          ? encode({
              wellheadId: query.wellheadId ?? null,
              well: target.wellheadId,
              rule: target.ruleId,
            })
          : null,
    });
  });
}

export async function readAlarmEvent(
  tx: Transaction,
  at: string,
  eventId: string,
  triggeredAt: string
) {
  const result = await tx.execute(
    sql`SELECT ${eventJson(at)} AS item ${eventFrom} WHERE e.event_id=${eventId}::bigint AND e.triggered_at=${triggeredAt}::timestamptz`
  );
  if (!result.rows[0])
    throw new HTTPException(404, {
      message: 'Alarm event is no longer retained or does not exist',
    });
  return alarmEventSchema.parse(result.rows[0].item);
}
export async function alarmDetail(eventId: string, triggeredAt: string) {
  return readSnapshot(async (tx, at) => {
    const event = await readAlarmEvent(tx, at, eventId, triggeredAt);
    const history = await tx.execute(
      sql`SELECT result FROM alarm_action WHERE wellhead_id=${event.target.wellheadId} AND alarm_rule_id=${event.target.ruleId} ORDER BY occurred_at DESC,action_id DESC LIMIT 20`
    );
    return alarmDetailSchema.parse({
      observedAt: at,
      event,
      actions: history.rows.map((row) => row.result),
    });
  });
}
export async function ownAlarmReceipt(actorId: number, requestKey: string) {
  const rows = await db.execute(
    sql`SELECT result FROM alarm_action WHERE actor_user_id=${actorId} AND request_key=${requestKey}::uuid`
  );
  if (!rows.rows[0])
    throw new HTTPException(404, {
      message: 'No committed action found for this request key',
    });
  return alarmReceiptSchema.parse(rows.rows[0].result);
}
