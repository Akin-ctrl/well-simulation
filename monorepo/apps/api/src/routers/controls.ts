import { Hono } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import { HTTPException } from 'hono/http-exception';
import {
  commandIdSchema,
  confirmationSchema,
  controlActivateSchema,
  controlDraftSchema,
  parameterDraftSchema,
  wellIdSchema,
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
} from '../control-service';
import { notifyTwin, runningControls } from '../twin-control-client';

const uuid = commandIdSchema;

async function requestBody(c: {
  req: { json: () => Promise<unknown> };
}): Promise<unknown> {
  try {
    return await c.req.json();
  } catch {
    throw new HTTPException(400, { message: 'Invalid JSON request' });
  }
}

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
      return c.json({ data, msg: 'Control state loaded', status: 200 });
    }
  )
  .post('/control-commands', requireCapability('control:issue'), async (c) => {
    const data = await draftCommand(
      c,
      controlDraftSchema.parse(await requestBody(c)),
      uuid.parse(c.req.header('Idempotency-Key'))
    );
    return c.json({ data, msg: 'Control proposal recorded', status: 201 }, 201);
  })
  .post(
    '/control-commands/:id/activate',
    requireCapability('control:issue'),
    async (c) => {
      const input = controlActivateSchema.parse(await requestBody(c));
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
          status,
        },
        status
      );
    }
  )
  .post(
    '/control-commands/:id/cancel',
    requireCapability('control:issue'),
    async (c) => {
      confirmationSchema.parse(await requestBody(c));
      const data = await changeCommand(c, uuid.parse(c.req.param('id')), 'cancel');
      return c.json({ data, msg: 'Control command status', status: 200 });
    }
  )
  .get('/control-commands/:id', requireCapability('control:issue'), async (c) => {
    const data = await commandStatus(c, uuid.parse(c.req.param('id')));
    return c.json({ data, msg: 'Control command status', status: 200 });
  })
  .post('/parameter-changes', requireCapability('asset:manage'), async (c) => {
    const data = await draftParameter(
      c,
      parameterDraftSchema.parse(await requestBody(c)),
      uuid.parse(c.req.header('Idempotency-Key'))
    );
    return c.json({ data, msg: 'Parameter proposal recorded', status: 201 }, 201);
  })
  .get(
    '/wellheads/:wellheadId/parameters',
    requireCapability('asset:manage'),
    async (c) => {
      const data = await parameterStatus(
        c,
        wellIdSchema.parse(Number(c.req.param('wellheadId')))
      );
      return c.json({ data, msg: 'Published and running parameters', status: 200 });
    }
  )
  .post(
    '/parameter-changes/:id/publish',
    requireCapability('asset:manage'),
    async (c) => {
      confirmationSchema.parse(await requestBody(c));
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
        status: 200,
      });
    }
  )
  .post(
    '/parameter-changes/:id/cancel',
    requireCapability('asset:manage'),
    async (c) => {
      confirmationSchema.parse(await requestBody(c));
      const data = await changeParameter(c, uuid.parse(c.req.param('id')), 'cancel');
      return c.json({ data, msg: 'Parameter proposal status', status: 200 });
    }
  )
  .get('/parameter-changes/:id', requireCapability('asset:manage'), async (c) => {
    const data = await changeParameter(c, uuid.parse(c.req.param('id')), 'read');
    const running = await runningControls(data.wellheadId).catch(() => null);
    return c.json({
      data: { ...data, running },
      msg: 'Parameter proposal status',
      status: 200,
    });
  });
