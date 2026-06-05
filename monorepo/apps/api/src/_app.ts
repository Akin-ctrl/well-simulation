import { Hono } from 'hono';
import { logger } from 'hono/logger';
import { prettyJSON } from 'hono/pretty-json';
import { authMiddleware } from './middleware/auth';
import { authRouter } from './routers/auth';
import { customRateLimit } from './middleware/rate-limiter';
import { dashboardRouter } from './routers/dashboard';

const app = new Hono();

app.use('/.well-known/*', async (c) => c.text('OK', 200));
app.get('/health', async (c) =>
  c.json({ status: 'ok', service: 'api', timestamp: new Date().toISOString() })
);
app.get('/ready', async (c) =>
  c.json({ status: 'ready', service: 'api', timestamp: new Date().toISOString() })
);

app.use(logger());

app.use('*', customRateLimit());

app.use(prettyJSON());
app.use('*', authMiddleware);

export const appRouter = app
  .route('/auth', authRouter)
  .route('/dashboard', dashboardRouter);

export type AppType = typeof appRouter;
