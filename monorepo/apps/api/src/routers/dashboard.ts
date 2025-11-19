import { Hono } from 'hono';
import { db } from '@corsight/db/query';
import { vWellheadParameterReadings } from '@corsight/db/schemas/views';

export const dashboardRouter = new Hono().get('/overview', async (c) => {
  const data = await db.select().from(vWellheadParameterReadings).limit(10);
  return c.json({ message: 'Auth route', data });
});
