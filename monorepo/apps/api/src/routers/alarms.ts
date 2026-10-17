import { Hono } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import { validator } from 'hono/validator';
import {
  alarmActionInputSchema,
  alarmIdentitySchema,
  alarmQuerySchema,
  alarmRequestKeySchema,
  alarmDetailQuerySchema,
  shelfQuerySchema,
} from '@well-simulation/dto/req/alarms';
import { requireCapability, sessionUserId } from '../middleware/authorize';
import { controlWriteLimit } from '../middleware/rate-limiter';
import { alarmDetail, listAlarms, listShelves, ownAlarmReceipt } from '../alarm-read';
import { applyAlarmAction } from '../alarm-write';

export const alarmsRouter = new Hono()
  .use('*', bodyLimit({ maxSize: 4096 }))
  .get(
    '/',
    requireCapability('dashboard:read'),
    validator('query', (value) => alarmQuerySchema.parse(value)),
    async (c) =>
      c.json({
        data: await listAlarms(c.req.valid('query')),
        msg: 'Alarm state loaded',
        status: 200 as const,
      })
  )
  .get(
    '/shelves',
    requireCapability('dashboard:read'),
    validator('query', (value) => shelfQuerySchema.parse(value)),
    async (c) =>
      c.json({
        data: await listShelves(c.req.valid('query')),
        msg: 'Current shelves loaded',
        status: 200 as const,
      })
  )
  .get(
    '/events/:eventId',
    requireCapability('dashboard:read'),
    validator('query', (value) => alarmDetailQuerySchema.parse(value)),
    async (c) => {
      const eventId = alarmIdentitySchema.shape.eventId.parse(c.req.param('eventId'));
      return c.json({
        data: await alarmDetail(eventId, c.req.valid('query').triggeredAt),
        msg: 'Event and recent actions loaded',
        status: 200 as const,
      });
    }
  )
  .get('/actions/:requestKey', requireCapability('dashboard:read'), async (c) =>
    c.json({
      data: await ownAlarmReceipt(
        sessionUserId(c),
        alarmRequestKeySchema.parse(c.req.param('requestKey'))
      ),
      msg: 'Your committed action',
      status: 200 as const,
    })
  )
  .post(
    '/actions',
    requireCapability('alarm:acknowledge'),
    (c, next) => controlWriteLimit(sessionUserId(c))(c, next),
    validator('json', (value) => alarmActionInputSchema.parse(value)),
    async (c) =>
      c.json({
        data: await applyAlarmAction(
          c,
          c.req.valid('json'),
          alarmRequestKeySchema.parse(c.req.header('Idempotency-Key'))
        ),
        msg: 'Alarm action recorded',
        status: 200 as const,
      })
  );
