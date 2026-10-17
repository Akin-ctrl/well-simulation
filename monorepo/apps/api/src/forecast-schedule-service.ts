import { errorCode } from './utils/database-error';
/** Schedule edits use the account-first lock order shared with the worker. */
import { randomUUID } from 'node:crypto';
import type { Context } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { and, db, desc, eq, sql } from '@well-simulation/db/query';
import {
  forecastSchedule,
  forecastJob,
} from '@well-simulation/db/schemas/forecast-schedules';
import { wellhead } from '@well-simulation/db/schemas/schema';
import { auditEvent } from '@well-simulation/db/schemas/audit';
import type { ForecastScheduleInput } from '@well-simulation/dto/req/forecast-schedule';
import {
  forecastScheduleViewSchema,
  forecastScheduleSchema,
} from '@well-simulation/dto/res/forecast-schedule';
import { currentActor } from './transaction-authority';
import { currentRequestId } from './middleware/request-id';

export async function readForecastSchedule(wellheadId: number) {
  const [asset] = await db
    .select({ id: wellhead.wellheadId })
    .from(wellhead)
    .where(eq(wellhead.wellheadId, wellheadId));
  if (!asset) throw new HTTPException(404, { message: 'Wellhead not found' });
  const [schedule, latest, completed] = await Promise.all([
    db
      .select()
      .from(forecastSchedule)
      .where(eq(forecastSchedule.wellheadId, wellheadId))
      .limit(1),
    db
      .select()
      .from(forecastJob)
      .where(eq(forecastJob.wellheadId, wellheadId))
      .orderBy(desc(forecastJob.startedAt), desc(forecastJob.jobId))
      .limit(1),
    db
      .select()
      .from(forecastJob)
      .where(
        and(eq(forecastJob.wellheadId, wellheadId), eq(forecastJob.status, 'complete'))
      )
      .orderBy(desc(forecastJob.completedAt), desc(forecastJob.jobId))
      .limit(1),
  ]);
  return forecastScheduleViewSchema.parse({
    schedule: schedule[0] ?? null,
    latestJob: latest[0] ?? null,
    latestCompleted: completed[0] ?? null,
  });
}

export async function saveForecastSchedule(
  c: Context,
  wellheadId: number,
  input: ForecastScheduleInput
) {
  try {
    return await db.transaction(async (tx) => {
      await tx.execute(sql`SET LOCAL lock_timeout = '500ms'`);
      await tx.execute(sql`SET LOCAL statement_timeout = '3000ms'`);
      const actor = await currentActor(tx, c, 'asset:manage');
      const [asset] = await tx
        .select()
        .from(wellhead)
        .where(eq(wellhead.wellheadId, wellheadId));
      if (!asset) throw new HTTPException(404, { message: 'Wellhead not found' });
      if (input.enabled && asset.status !== 'active')
        throw new HTTPException(409, {
          message: 'Activate this well before enabling its schedule',
        });
      const [current] = await tx
        .select()
        .from(forecastSchedule)
        .where(eq(forecastSchedule.wellheadId, wellheadId))
        .for('update');
      if ((current?.revision ?? 0) !== input.expectedRevision)
        throw new HTTPException(409, {
          message: 'Schedule changed; read it again before saving',
        });
      const values = {
        ownerUserId: actor.id,
        ownerRole: 'ADMIN',
        enabled: input.enabled,
        intervalSeconds: input.intervalMinutes * 60,
        horizonSeconds: input.durationMinutes * 60,
        stepSeconds: 60,
        revision: input.expectedRevision + 1,
        reason: input.reason,
        updatedAt: sql`clock_timestamp()`,
        nextDueAt: input.enabled
          ? sql`clock_timestamp() + ${input.intervalMinutes} * interval '1 minute'`
          : null,
      };
      const [saved] = current
        ? await tx
            .update(forecastSchedule)
            .set(values)
            .where(eq(forecastSchedule.scheduleId, current.scheduleId))
            .returning()
        : await tx
            .insert(forecastSchedule)
            .values({ ...values, scheduleId: randomUUID(), wellheadId })
            .returning();
      if (!saved) throw new Error('Schedule save returned no row');
      await tx.insert(auditEvent).values({
        occurredAt: sql`clock_timestamp()`,
        actorUserId: actor.id,
        action: 'forecast_schedule.updated',
        subjectType: 'forecast_schedule',
        subjectId: saved.scheduleId,
        outcome: 'success',
        requestId: currentRequestId(c),
        detail: {
          wellheadId,
          revision: saved.revision,
          enabled: saved.enabled,
          intervalSeconds: saved.intervalSeconds,
          horizonSeconds: saved.horizonSeconds,
          reason: saved.reason,
        },
      });
      return forecastScheduleSchema.parse(saved);
    });
  } catch (error) {
    if (error instanceof HTTPException) throw error;
    if (errorCode(error) === '23505')
      throw new HTTPException(409, {
        message: 'Schedule changed; read it again before saving',
      });
    console.error(
      JSON.stringify({
        level: 'error',
        service: 'api',
        event: 'forecast_schedule_write_failed',
        requestId: currentRequestId(c),
      })
    );
    throw new HTTPException(503, {
      message: 'Save outcome unavailable; read the schedule before retrying',
    });
  }
}
