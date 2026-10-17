import type { Context } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { db, eq } from '@well-simulation/db/query';
import { users } from '@well-simulation/db/schemas/user';
import {
  hasCapability,
  isRole,
  type Capability,
} from '@well-simulation/dto/auth/roles';
import { sessionUserId } from './middleware/authorize';
export type Transaction = Parameters<Parameters<typeof db.transaction>[0]>[0];

/** Lock the current account, not the potentially stale role in its JWT. */
export async function currentActor(
  tx: Transaction,
  c: Context,
  capability: Capability
) {
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
