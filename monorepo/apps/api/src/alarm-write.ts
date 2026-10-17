import { createHash, randomUUID } from 'node:crypto';
import type { Context } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { and, db, eq, sql } from '@well-simulation/db/query';
import {
  alarmAction,
  alarmAcknowledgement,
  alarmShelf,
} from '@well-simulation/db/schemas/alarms';
import { auditEvent } from '@well-simulation/db/schemas/audit';
import type { AlarmActionInput } from '@well-simulation/dto/req/alarms';
import { alarmReceiptSchema, alarmTargetSchema } from '@well-simulation/dto/res/alarms';
import { currentActor } from './transaction-authority';
import { currentRequestId } from './middleware/request-id';
import { errorCode } from './utils/database-error';
import { exactTime, readAlarmEvent, targetJson } from './alarm-read';

const conflict = () =>
  new HTTPException(409, {
    message: 'Alarm state changed. Read it again and review your action.',
  });
const auditActions = {
  acknowledge: 'alarm.acknowledged',
  shelve: 'alarm.shelved',
  unshelve: 'alarm.unshelved',
} as const;

/** Commit the receipt, lifecycle state and audit together under current authority. */
export async function applyAlarmAction(
  c: Context,
  input: AlarmActionInput,
  requestKey: string
) {
  const inputHash = createHash('sha256').update(JSON.stringify(input)).digest('hex');
  try {
    return await db.transaction(async (tx) => {
      await tx.execute(sql`SET LOCAL lock_timeout = '500ms'`);
      await tx.execute(sql`SET LOCAL statement_timeout = '3000ms'`);
      const actor = await currentActor(tx, c, 'alarm:acknowledge');
      const [previous] = await tx
        .select()
        .from(alarmAction)
        .where(
          and(
            eq(alarmAction.actorUserId, actor.id),
            eq(alarmAction.requestKey, requestKey)
          )
        );
      if (previous) {
        if (previous.inputHash !== inputHash)
          throw new HTTPException(409, {
            message: 'This request key already belongs to different reviewed input',
          });
        return alarmReceiptSchema.parse(previous.result);
      }
      const initialClock = await tx.execute<{ at: string }>(
        sql`SELECT ${exactTime(sql`clock_timestamp()`)} AS at`
      );
      const event =
        input.action === 'acknowledge'
          ? await readAlarmEvent(
              tx,
              initialClock.rows[0]!.at,
              input.eventId,
              input.triggeredAt
            )
          : null;
      if (event?.acknowledgement) throw conflict();
      const target =
        event?.target ??
        (await (async () => {
          if (input.action === 'acknowledge') throw new Error('Missing event target');
          const row =
            await tx.execute(sql`SELECT ${targetJson} AS target FROM wellhead w
          JOIN location l ON l.location_id=w.location_id CROSS JOIN alarmrule r
          JOIN parametertype p ON p.parameter_type_id=r.parameter_type_id
          WHERE w.wellhead_id=${input.wellheadId} AND r.alarm_rule_id=${input.ruleId}`);
          if (!row.rows[0])
            throw new HTTPException(404, { message: 'Well or alarm rule not found' });
          return alarmTargetSchema.parse(row.rows[0].target);
        })());
      const shelfKey = and(
        eq(alarmShelf.wellheadId, target.wellheadId),
        eq(alarmShelf.ruleId, target.ruleId)
      );
      const current =
        input.action === 'acknowledge'
          ? undefined
          : (await tx.select().from(alarmShelf).where(shelfKey).for('update'))[0];
      if (
        input.action !== 'acknowledge' &&
        (current?.revision ?? 0) !== input.expectedRevision
      )
        throw conflict();
      // Calculate the deadline after locks, once, so the receipt and state agree.
      const duration = input.action === 'shelve' ? input.durationMinutes : 0;
      const clock = await tx.execute<{
        at: string;
        until: string;
        active: boolean;
      }>(sql`WITH clock AS (SELECT clock_timestamp() AS at)
        SELECT ${exactTime(sql`at`)} AS at,${exactTime(sql`at + ${duration} * interval '1 minute'`)} AS until,
          coalesce(${current?.shelvedUntil ?? null}::timestamptz > at,false) AS active FROM clock`);
      const now = clock.rows[0]!;
      if (input.action === 'unshelve' && !now.active) throw conflict();
      const receipt = alarmReceiptSchema.parse({
        actionId: randomUUID(),
        action: input.action,
        occurredAt: now.at,
        actorUserId: actor.id,
        actorName: actor.userName,
        reason: input.reason,
        target,
        eventId: event?.eventId ?? null,
        triggeredAt: event?.triggeredAt ?? null,
        revision: input.action === 'acknowledge' ? null : input.expectedRevision + 1,
        shelvedUntil: input.action === 'shelve' ? now.until : null,
      });
      await tx.insert(alarmAction).values({
        actionId: receipt.actionId,
        actorUserId: actor.id,
        actorName: actor.userName,
        actorRole: 'OPERATIONS',
        requestKey,
        inputHash,
        action: input.action,
        wellheadId: target.wellheadId,
        ruleId: target.ruleId,
        occurredAt: receipt.occurredAt,
        input,
        result: receipt,
      });
      if (input.action === 'acknowledge') {
        await tx.insert(alarmAcknowledgement).values({
          eventId: BigInt(input.eventId),
          triggeredAt: input.triggeredAt,
          acknowledgedAt: receipt.occurredAt,
          actionId: receipt.actionId,
        });
      } else {
        const values = {
          revision: input.expectedRevision + 1,
          shelvedUntil: receipt.shelvedUntil,
          updatedAt: receipt.occurredAt,
          actionId: receipt.actionId,
        };
        if (current) await tx.update(alarmShelf).set(values).where(shelfKey);
        else
          await tx.insert(alarmShelf).values({
            ...values,
            wellheadId: target.wellheadId,
            ruleId: target.ruleId,
          });
      }
      await tx.insert(auditEvent).values({
        occurredAt: receipt.occurredAt,
        actorUserId: actor.id,
        action: auditActions[input.action],
        subjectType: 'alarm_action',
        subjectId: receipt.actionId,
        outcome: 'success',
        requestId: currentRequestId(c),
        detail: receipt,
      });
      return receipt;
    });
  } catch (error) {
    if (error instanceof HTTPException) throw error;
    if (errorCode(error) === '23505') throw conflict();
    console.error(
      JSON.stringify({
        level: 'error',
        service: 'api',
        event: 'alarm_action_failed',
        requestId: currentRequestId(c),
        code: errorCode(error),
      })
    );
    throw new HTTPException(503, {
      message:
        'The action outcome is unavailable. Check this request key before retrying.',
    });
  }
}
