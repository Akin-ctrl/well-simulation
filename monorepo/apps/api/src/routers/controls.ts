import { Hono } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import { validator } from 'hono/validator';
import {
  commandIdSchema,
  confirmationSchema,
  controlActivateSchema,
  controlDraftSchema,
  parameterDraftSchema,
  wellIdSchema,
  proposalHistoryQuerySchema,
} from '@well-simulation/dto/req/controls';
import { requireCapability, sessionUserId } from '../middleware/authorize';
import { controlWriteLimit } from '../middleware/rate-limiter';
import {
  changeCommand,
  changeParameter,
  commandStatus,
  controlStatus,
  draftCommand,
  draftParameter,
  parameterStatus,
  proposalHistory,
} from '../control-service';
import { notifyTwin, runningControls } from '../twin-control-client';

const uuid = commandIdSchema;

/** Human command routes never accept a service token from the browser. */
export const controlsRouter = new Hono()
  .use('*', bodyLimit({ maxSize: 4096 }))
  .use('*', async (c, next) => {
    if (c.req.method === 'POST') return controlWriteLimit(sessionUserId(c))(c, next);
    return next();
  })
  .get(
    '/wellheads/:wellheadId/controls',
    requireCapability('dashboard:read'),
    async (c) => {
      const data = await controlStatus(
        wellIdSchema.parse(Number(c.req.param('wellheadId')))
      );
      return c.json({ data, msg: 'Control state loaded', status: 200 as const });
    }
  )
  .get(
    '/wellheads/:wellheadId/control-commands',
    requireCapability('control:issue'),
    validator('query', (value) => proposalHistoryQuerySchema.parse(value)),
    async (c) => {
      const data = await proposalHistory(
        c,
        wellIdSchema.parse(Number(c.req.param('wellheadId'))),
        'control',
        c.req.valid('query')
      );
      return c.json({ data, msg: 'Your control proposals', status: 200 as const });
    }
  )
  .get(
    '/wellheads/:wellheadId/parameter-changes',
    requireCapability('asset:manage'),
    validator('query', (value) => proposalHistoryQuerySchema.parse(value)),
    async (c) => {
      const data = await proposalHistory(
        c,
        wellIdSchema.parse(Number(c.req.param('wellheadId'))),
        'parameter',
        c.req.valid('query')
      );
      return c.json({ data, msg: 'Your parameter proposals', status: 200 as const });
    }
  )
  .post(
    '/control-commands',
    requireCapability('control:issue'),
    validator('json', (value) => controlDraftSchema.parse(value)),
    async (c) => {
      const data = await draftCommand(
        c,
        c.req.valid('json'),
        uuid.parse(c.req.header('Idempotency-Key'))
      );
      return c.json(
        { data, msg: 'Control proposal recorded', status: 201 as const },
        201
      );
    }
  )
  .post(
    '/control-commands/:id/activate',
    requireCapability('control:issue'),
    validator('json', (value) => controlActivateSchema.parse(value)),
    async (c) => {
      const input = c.req.valid('json');
      const data = await changeCommand(
        c,
        uuid.parse(c.req.param('id')),
        'activate',
        input.expectedRevision
      );
      const notified =
        data.status === 'queued'
          ? await notifyTwin(`/control-commands/${data.commandId}/notify`)
          : true;
      const status = data.status === 'queued' ? 202 : 200;
      return c.json(
        {
          data: { ...data, notificationPending: !notified },
          msg: 'Control command status',
          status: status as 200 | 202,
        },
        status
      );
    }
  )
  .post(
    '/control-commands/:id/cancel',
    requireCapability('control:issue'),
    validator('json', (value) => confirmationSchema.parse(value)),
    async (c) => {
      const data = await changeCommand(c, uuid.parse(c.req.param('id')), 'cancel');
      return c.json({ data, msg: 'Control command status', status: 200 as const });
    }
  )
  .get('/control-commands/:id', requireCapability('control:issue'), async (c) => {
    const data = await commandStatus(c, uuid.parse(c.req.param('id')));
    return c.json({ data, msg: 'Control command status', status: 200 as const });
  })
  .post(
    '/parameter-changes',
    requireCapability('asset:manage'),
    validator('json', (value) => parameterDraftSchema.parse(value)),
    async (c) => {
      const data = await draftParameter(
        c,
        c.req.valid('json'),
        uuid.parse(c.req.header('Idempotency-Key'))
      );
      return c.json(
        { data, msg: 'Parameter proposal recorded', status: 201 as const },
        201
      );
    }
  )
  .get(
    '/wellheads/:wellheadId/parameters',
    requireCapability('asset:manage'),
    async (c) => {
      const data = await parameterStatus(
        c,
        wellIdSchema.parse(Number(c.req.param('wellheadId')))
      );
      return c.json({
        data,
        msg: 'Published and running parameters',
        status: 200 as const,
      });
    }
  )
  .post(
    '/parameter-changes/:id/publish',
    requireCapability('asset:manage'),
    validator('json', (value) => confirmationSchema.parse(value)),
    async (c) => {
      const data = await changeParameter(c, uuid.parse(c.req.param('id')), 'publish');
      const notified =
        data.status === 'published' ? await notifyTwin('/fleet/refresh') : true;
      return c.json({
        data: {
          ...data,
          restartRequired: data.status === 'published',
          notificationPending: !notified,
        },
        msg: 'Parameter proposal status',
        status: 200 as const,
      });
    }
  )
  .post(
    '/parameter-changes/:id/cancel',
    requireCapability('asset:manage'),
    validator('json', (value) => confirmationSchema.parse(value)),
    async (c) => {
      const data = await changeParameter(c, uuid.parse(c.req.param('id')), 'cancel');
      return c.json({ data, msg: 'Parameter proposal status', status: 200 as const });
    }
  )
  .get('/parameter-changes/:id', requireCapability('asset:manage'), async (c) => {
    const data = await changeParameter(c, uuid.parse(c.req.param('id')), 'read');
    const running = await runningControls(data.wellheadId).catch(() => null);
    return c.json({
      data: { ...data, running },
      msg: 'Parameter proposal status',
      status: 200 as const,
    });
  });
