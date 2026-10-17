/** Atomic command and parameter lifecycles, with current authority and audit. */
import { randomUUID } from 'node:crypto';
import type { Context } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { and, asc, db, desc, eq, sql } from '@well-simulation/db/query';
import { users } from '@well-simulation/db/schemas/user';
import { auditEvent } from '@well-simulation/db/schemas/audit';
import { wellhead, wellheadModelParameter } from '@well-simulation/db/schemas/schema';
import {
  controlCommand,
  controlCommandEvent,
  modelParameterChange,
  wellheadControlState,
} from '@well-simulation/db/schemas/controls';
import {
  hasCapability,
  isRole,
  type Capability,
} from '@well-simulation/dto/auth/roles';
import type { ControlDraft, ParameterDraft } from '@well-simulation/dto/req/controls';
import {
  runningControls,
  parameterIdentity,
  parameterCatalogue,
  validateParameter,
} from './twin-control-client';
import { sessionUserId } from './middleware/authorize';
import { currentRequestId } from './middleware/request-id';
import {
  commandSummarySchema,
  parameterSummarySchema,
} from '@well-simulation/dto/res/controls';

type Transaction = Parameters<Parameters<typeof db.transaction>[0]>[0];
type Command = typeof controlCommand.$inferSelect;
type ParameterChange = typeof modelParameterChange.$inferSelect;

function requireRunning(state: Awaited<ReturnType<typeof runningControls>>): void {
  if (
    state.status !== 'running' ||
    state.lastTickAt === null ||
    Math.abs(Date.now() - Date.parse(state.lastTickAt)) > 3000
  )
    throw new HTTPException(503, { message: 'Model is not current' });
}

/** Lock the current account, not the potentially stale role in its JWT. */
async function currentActor(tx: Transaction, c: Context, capability: Capability) {
  const id = sessionUserId(c);
  const [actor] = await tx.select().from(users).where(eq(users.id, id)).for('update');
  const claims = c.get('session') as Record<string, unknown>;
  if (!actor || actor.sessionVersion !== claims.sv)
    throw new HTTPException(401, { message: 'Session revoked' });
  if (
    actor.status !== 'ACTIVE' ||
    actor.deleted === true ||
    !isRole(actor.role) ||
    !hasCapability(actor.role, capability)
  )
    throw new HTTPException(403, { message: 'Forbidden' });
  return actor;
}

/** Transactional audit: throwing here rolls back the accepted change too. */
async function audit(
  tx: Transaction,
  c: Context,
  action: string,
  subjectType: string,
  subjectId: string
): Promise<void> {
  await tx.insert(auditEvent).values({
    occurredAt: sql`clock_timestamp()`,
    actorUserId: sessionUserId(c),
    action,
    subjectType,
    subjectId,
    outcome: 'success',
    requestId: currentRequestId(c),
    detail: {},
  });
}

async function commandEvent(
  tx: Transaction,
  c: Context,
  commandId: string,
  eventType: string,
  reasonCode: string
): Promise<void> {
  await tx.insert(controlCommandEvent).values({
    commandId,
    eventType,
    actorUserId: sessionUserId(c),
    reasonCode,
    requestId: currentRequestId(c),
  });
  await audit(tx, c, `control.${eventType}`, 'control_command', commandId);
}

function errorCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const value = error as { code?: unknown; cause?: unknown };
  return typeof value.code === 'string'
    ? value.code
    : value.cause
      ? errorCode(value.cause)
      : undefined;
}

/** Avoid driver errors containing a command reason or a credential in logs. */
async function transaction<T>(
  c: Context,
  operation: (tx: Transaction) => Promise<T>
): Promise<T> {
  try {
    return await db.transaction(async (tx) => {
      await tx.execute(sql`SET LOCAL lock_timeout = '500ms'`);
      await tx.execute(sql`SET LOCAL statement_timeout = '3000ms'`);
      return operation(tx);
    });
  } catch (error) {
    if (error instanceof HTTPException) throw error;
    const code = errorCode(error);
    console.error(
      JSON.stringify({
        level: 'error',
        service: 'api',
        event: 'control_transaction_failed',
        code,
        requestId: currentRequestId(c),
      })
    );
    if (code === '23505')
      throw new HTTPException(409, {
        message: 'A conflicting proposal or queued command exists',
      });
    throw new HTTPException(503, {
      message: 'Write outcome unavailable; read the proposal status before retrying',
    });
  }
}

async function activeRow(tx: Transaction, wellheadId: number) {
  await tx.execute(sql`SELECT lock_control_state(${wellheadId})`);
  const [row] = await tx
    .select()
    .from(wellheadControlState)
    .where(eq(wellheadControlState.wellheadId, wellheadId));
  if (!row)
    throw new HTTPException(503, { message: 'Active controls are not initialized' });
  return row;
}

function assertSameDraft(row: Command, input: ControlDraft): void {
  const targets = input.targets;
  if (
    row.wellheadId !== input.wellheadId ||
    row.expectedRevision !== input.expectedRevision ||
    row.reason !== input.reason ||
    row.chokePositionPercent !== (targets.chokePositionPercent ?? null) ||
    row.masterValveOpen !== (targets.masterValveOpen ?? null) ||
    row.wingValveOpen !== (targets.wingValveOpen ?? null) ||
    row.pumpOn !== (targets.pumpOn ?? null) ||
    row.emergencyShutdown !== (targets.emergencyShutdown ?? null)
  )
    throw new HTTPException(409, {
      message: 'Idempotency key already used for a different proposal',
    });
}

async function expireCommand(
  tx: Transaction,
  c: Context,
  row: Command
): Promise<Command> {
  const [expired] = await tx
    .update(controlCommand)
    .set({
      status: 'expired',
      resultAt: sql`clock_timestamp()`,
      resultReason: 'expired',
    })
    .where(
      and(
        eq(controlCommand.commandId, row.commandId),
        sql`${controlCommand.expiresAt} <= clock_timestamp()`,
        sql`${controlCommand.status} IN ('draft','queued')`
      )
    )
    .returning();
  if (expired) await commandEvent(tx, c, row.commandId, 'expired', 'expired');
  return expired ?? row;
}

export async function draftCommand(
  c: Context,
  input: ControlDraft,
  idempotencyKey: string
): Promise<Command> {
  return transaction(c, async (tx) => {
    const actor = await currentActor(tx, c, 'control:issue');
    const [existing] = await tx
      .select()
      .from(controlCommand)
      .where(
        and(
          eq(controlCommand.createdBy, actor.id),
          eq(controlCommand.idempotencyKey, idempotencyKey)
        )
      )
      .for('update');
    if (existing) {
      assertSameDraft(existing, input);
      return expireCommand(tx, c, existing);
    }
    const prior = await tx
      .select()
      .from(controlCommand)
      .where(
        and(
          eq(controlCommand.createdBy, actor.id),
          eq(controlCommand.wellheadId, input.wellheadId),
          eq(controlCommand.status, 'draft')
        )
      )
      .for('update');
    for (const row of prior) await expireCommand(tx, c, row);
    const running = await runningControls(input.wellheadId);
    requireRunning(running);
    const active = await activeRow(tx, input.wellheadId);
    if (
      active.revision !== input.expectedRevision ||
      running.revision !== active.revision ||
      active.modelVersion !== running.modelVersion ||
      active.parameterFingerprint !== running.parameterFingerprint
    )
      throw new HTTPException(409, {
        message: 'Active controls changed; refresh before drafting',
      });
    const [row] = await tx
      .insert(controlCommand)
      .values({
        commandId: randomUUID(),
        createdBy: actor.id,
        idempotencyKey,
        wellheadId: input.wellheadId,
        reason: input.reason,
        expectedRevision: input.expectedRevision,
        modelVersion: running.modelVersion,
        parameterFingerprint: running.parameterFingerprint,
        ...input.targets,
        requestId: currentRequestId(c),
      })
      .returning();
    if (!row) throw new Error('Draft insert failed');
    await commandEvent(tx, c, row.commandId, 'draft', 'created');
    return row;
  });
}

export async function changeCommand(
  c: Context,
  commandId: string,
  action: 'activate' | 'cancel',
  expectedRevision?: number
): Promise<Command> {
  return transaction(c, async (tx) => {
    const actor = await currentActor(tx, c, 'control:issue');
    const [found] = await tx
      .select()
      .from(controlCommand)
      .where(eq(controlCommand.commandId, commandId))
      .for('update');
    if (!found) throw new HTTPException(404, { message: 'Command not found' });
    if (found.createdBy !== actor.id)
      throw new HTTPException(403, {
        message: 'Only the creator may change this command',
      });
    const row = await expireCommand(tx, c, found);
    if (row.status !== 'draft' && (action === 'activate' || row.status !== 'queued'))
      return row;
    if (action === 'cancel') {
      const [cancelled] = await tx
        .update(controlCommand)
        .set({
          status: 'cancelled',
          resultAt: sql`clock_timestamp()`,
          resultReason: 'creator_cancelled',
        })
        .where(eq(controlCommand.commandId, commandId))
        .returning();
      await commandEvent(tx, c, commandId, 'cancelled', 'creator_cancelled');
      return cancelled!;
    }
    const running = await runningControls(row.wellheadId);
    requireRunning(running);
    const active = await activeRow(tx, row.wellheadId);
    if (
      row.expectedRevision !== expectedRevision ||
      active.revision !== expectedRevision ||
      running.revision !== expectedRevision ||
      row.modelVersion !== running.modelVersion ||
      row.parameterFingerprint !== running.parameterFingerprint ||
      row.modelVersion !== active.modelVersion ||
      row.parameterFingerprint !== active.parameterFingerprint
    )
      throw new HTTPException(409, { message: 'Model or active revision changed' });
    if (active.emergencyShutdown && row.emergencyShutdown === null)
      throw new HTTPException(409, {
        message: 'Reset emergency shutdown in a separate command first',
      });
    const [queued] = await tx
      .update(controlCommand)
      .set({
        status: 'queued',
        queuedAt: sql`clock_timestamp()`,
        expiresAt: sql`clock_timestamp() + interval '30 seconds'`,
        activatedBy: actor.id,
        activatedRole: actor.role,
        activatedSessionVersion: actor.sessionVersion,
      })
      .where(eq(controlCommand.commandId, commandId))
      .returning();
    await commandEvent(tx, c, commandId, 'queued', 'activated');
    return queued!;
  });
}

export async function commandStatus(c: Context, commandId: string) {
  return transaction(c, async (tx) => {
    await currentActor(tx, c, 'control:issue');
    const [found] = await tx
      .select()
      .from(controlCommand)
      .where(eq(controlCommand.commandId, commandId))
      .for('update');
    if (!found) throw new HTTPException(404, { message: 'Command not found' });
    const row = await expireCommand(tx, c, found);
    const events = await tx
      .select()
      .from(controlCommandEvent)
      .where(eq(controlCommandEvent.commandId, commandId))
      .orderBy(asc(controlCommandEvent.eventId));
    return { ...row, events };
  });
}

export async function controlStatus(wellheadId: number) {
  const running = await runningControls(wellheadId);
  const [active] = await db
    .select()
    .from(wellheadControlState)
    .where(eq(wellheadControlState.wellheadId, wellheadId));
  const [queued] = await db
    .select({ commandId: controlCommand.commandId })
    .from(controlCommand)
    .where(
      and(
        eq(controlCommand.wellheadId, wellheadId),
        eq(controlCommand.status, 'queued')
      )
    );
  return {
    active: active ?? null,
    running,
    pendingCommandId: queued?.commandId ?? null,
  };
}

/** Let an administrator obtain revision IDs without hand-entered SQL. */
export async function parameterStatus(c: Context, wellheadId: number) {
  await transaction(c, (tx) => currentActor(tx, c, 'asset:manage'));
  // Network waits must not hold the account lock needed by outcome recovery.
  const [identity, catalogue, running] = await Promise.all([
    parameterIdentity(wellheadId),
    parameterCatalogue(wellheadId),
    runningControls(wellheadId).catch(() => null),
  ]);
  if (identity.modelVersion !== catalogue.modelVersion)
    throw new HTTPException(503, {
      message: 'Model version changed; refresh the view',
    });
  return transaction(c, async (tx) => {
    await currentActor(tx, c, 'asset:manage');
    const overrides = await tx
      .selectDistinctOn([wellheadModelParameter.parameterKey])
      .from(wellheadModelParameter)
      .where(
        and(
          eq(wellheadModelParameter.wellheadId, wellheadId),
          eq(wellheadModelParameter.modelVersion, identity.modelVersion)
        )
      )
      .orderBy(
        asc(wellheadModelParameter.parameterKey),
        desc(wellheadModelParameter.validFrom),
        desc(wellheadModelParameter.parameterOverrideId)
      );
    return { ...identity, latestOverrides: overrides, running, catalogue };
  });
}

/** Recover a person's proposals without requiring an available model service. */
export async function proposalHistory(
  c: Context,
  wellheadId: number,
  kind: 'control' | 'parameter',
  query: { offset: number; idempotencyKey?: string }
) {
  return transaction(c, async (tx) => {
    const actor = await currentActor(
      tx,
      c,
      kind === 'control' ? 'control:issue' : 'asset:manage'
    );
    const table = kind === 'control' ? controlCommand : modelParameterChange;
    const id =
      kind === 'control' ? controlCommand.commandId : modelParameterChange.changeId;
    const rows = await tx
      .select()
      .from(table)
      .where(
        and(
          eq(table.wellheadId, wellheadId),
          eq(table.createdBy, actor.id),
          query.idempotencyKey
            ? eq(table.idempotencyKey, query.idempotencyKey)
            : undefined
        )
      )
      .orderBy(desc(table.createdAt), desc(id))
      .limit(21)
      .offset(query.offset);
    const items = rows
      .slice(0, 20)
      .map((row) =>
        kind === 'control'
          ? commandSummarySchema.parse(row)
          : parameterSummarySchema.parse(row)
      );
    return { items, offset: query.offset, hasMore: rows.length > 20 };
  });
}

async function latestOverride(
  tx: Transaction,
  input: Pick<ParameterDraft, 'wellheadId' | 'modelVersion' | 'parameterKey'>
) {
  const [row] = await tx
    .select()
    .from(wellheadModelParameter)
    .where(
      and(
        eq(wellheadModelParameter.wellheadId, input.wellheadId),
        eq(wellheadModelParameter.modelVersion, input.modelVersion),
        eq(wellheadModelParameter.parameterKey, input.parameterKey)
      )
    )
    .orderBy(
      desc(wellheadModelParameter.validFrom),
      desc(wellheadModelParameter.parameterOverrideId)
    )
    .limit(1);
  return row?.parameterOverrideId ?? null;
}

function assertSameParameter(row: ParameterChange, input: ParameterDraft): void {
  if (
    row.wellheadId !== input.wellheadId ||
    row.modelVersion !== input.modelVersion ||
    row.parameterKey !== input.parameterKey ||
    row.value !== input.value ||
    row.unit !== input.unit ||
    row.reason !== input.reason ||
    row.expectedOverrideId !== input.expectedOverrideId
  )
    throw new HTTPException(409, {
      message: 'Idempotency key already used for a different proposal',
    });
}

async function expireParameter(
  tx: Transaction,
  c: Context,
  row: ParameterChange
): Promise<ParameterChange> {
  const [expired] = await tx
    .update(modelParameterChange)
    .set({ status: 'expired', resultAt: sql`clock_timestamp()` })
    .where(
      and(
        eq(modelParameterChange.changeId, row.changeId),
        eq(modelParameterChange.status, 'draft'),
        sql`${modelParameterChange.expiresAt} <= clock_timestamp()`
      )
    )
    .returning();
  if (expired)
    await audit(tx, c, 'parameter.expired', 'model_parameter_change', row.changeId);
  return expired ?? row;
}

export async function draftParameter(
  c: Context,
  input: ParameterDraft,
  idempotencyKey: string
): Promise<ParameterChange> {
  return transaction(c, async (tx) => {
    const actor = await currentActor(tx, c, 'asset:manage');
    const [existing] = await tx
      .select()
      .from(modelParameterChange)
      .where(
        and(
          eq(modelParameterChange.createdBy, actor.id),
          eq(modelParameterChange.idempotencyKey, idempotencyKey)
        )
      )
      .for('update');
    if (existing) {
      assertSameParameter(existing, input);
      return expireParameter(tx, c, existing);
    }
    const prior = await tx
      .select()
      .from(modelParameterChange)
      .where(
        and(
          eq(modelParameterChange.createdBy, actor.id),
          eq(modelParameterChange.wellheadId, input.wellheadId),
          eq(modelParameterChange.parameterKey, input.parameterKey),
          eq(modelParameterChange.status, 'draft')
        )
      )
      .for('update');
    for (const row of prior) await expireParameter(tx, c, row);
    await tx.execute(sql`SELECT pg_advisory_xact_lock(8038, ${input.wellheadId})`);
    if ((await latestOverride(tx, input)) !== input.expectedOverrideId)
      throw new HTTPException(409, { message: 'Parameter override changed' });
    await validateParameter(input);
    const [row] = await tx
      .insert(modelParameterChange)
      .values({ ...input, changeId: randomUUID(), createdBy: actor.id, idempotencyKey })
      .returning();
    if (!row) throw new Error('Parameter draft insert failed');
    await audit(tx, c, 'parameter.draft', 'model_parameter_change', row.changeId);
    return row;
  });
}

export async function changeParameter(
  c: Context,
  changeId: string,
  action: 'publish' | 'cancel' | 'read'
) {
  return transaction(c, async (tx) => {
    const actor = await currentActor(tx, c, 'asset:manage');
    const [found] = await tx
      .select()
      .from(modelParameterChange)
      .where(eq(modelParameterChange.changeId, changeId))
      .for('update');
    if (!found) throw new HTTPException(404, { message: 'Parameter change not found' });
    if (action !== 'read' && found.createdBy !== actor.id)
      throw new HTTPException(403, {
        message: 'Only the creator may change this proposal',
      });
    const row = await expireParameter(tx, c, found);
    if (row.status !== 'draft' || action === 'read') return row;
    if (action === 'cancel') {
      const [cancelled] = await tx
        .update(modelParameterChange)
        .set({ status: 'cancelled', resultAt: sql`clock_timestamp()` })
        .where(eq(modelParameterChange.changeId, changeId))
        .returning();
      await audit(tx, c, 'parameter.cancelled', 'model_parameter_change', changeId);
      return cancelled!;
    }
    await tx.execute(sql`SELECT pg_advisory_xact_lock(8038, ${row.wellheadId})`);
    const [well] = await tx
      .select()
      .from(wellhead)
      .where(
        and(eq(wellhead.wellheadId, row.wellheadId), eq(wellhead.status, 'active'))
      );
    if (!well) throw new HTTPException(409, { message: 'Well is no longer active' });
    if ((await latestOverride(tx, row)) !== row.expectedOverrideId)
      throw new HTTPException(409, { message: 'Parameter override changed' });
    await validateParameter(row);
    const [override] = await tx
      .insert(wellheadModelParameter)
      .values({
        wellheadId: row.wellheadId,
        modelVersion: row.modelVersion,
        parameterKey: row.parameterKey,
        value: row.value,
        unit: row.unit,
        source: 'authenticated_api',
        changedBy: String(actor.id),
        reason: row.reason,
        validFrom: sql`clock_timestamp()`,
      })
      .returning();
    if (!override) throw new Error('Override insert failed');
    const [published] = await tx
      .update(modelParameterChange)
      .set({
        status: 'published',
        resultAt: sql`clock_timestamp()`,
        publishedOverrideId: override.parameterOverrideId,
      })
      .where(eq(modelParameterChange.changeId, changeId))
      .returning();
    await audit(tx, c, 'parameter.published', 'model_parameter_change', changeId);
    return published!;
  });
}
