import { Hono } from 'hono';
import { logger } from 'hono/logger';
import { prettyJSON } from 'hono/pretty-json';
import { secureHeaders } from 'hono/secure-headers';
import { db, sql } from '@well-simulation/db/query';
import { authMiddleware } from './middleware/auth';
import { authRouter } from './routers/auth';
import { customRateLimit } from './middleware/rate-limiter';
import { originGuard } from './middleware/csrf';
import { requestId } from './middleware/request-id';
import { dashboardRouter } from './routers/dashboard';
import { errorResponse } from './utils/handler';
import { httpDuration, httpRequests, registry } from './metrics';

const app = new Hono();

// Middleware order is load-bearing. Anything registered after a route does not
// apply to it, so the cross-cutting concerns come first and the unauthenticated
// probes are mounted between them and the authentication gate. Previously the
// health routes were declared first and so bypassed every middleware, including
// rate limiting.
// First, so every later log line and audit row can carry the id.
app.use('*', requestId());

app.use(logger());

// Recorded for every request, including the ones that never reach a router.
app.use('*', async (c, next) => {
  const stop = httpDuration.startTimer({
    method: c.req.method,
    route: c.req.routePath,
  });
  await next();
  stop();
  httpRequests.inc({
    method: c.req.method,
    route: c.req.routePath,
    status: String(c.res.status),
  });
});

// Defence in depth: nginx sets these for the dashboard, but the API must not
// depend on being behind it.
app.use(
  '*',
  secureHeaders({
    contentSecurityPolicy: { defaultSrc: ["'none'"], frameAncestors: ["'none'"] },
    xFrameOptions: 'DENY',
    xContentTypeOptions: 'nosniff',
    referrerPolicy: 'no-referrer',
  })
);

app.use('*', customRateLimit());

// Liveness and readiness are deliberately unauthenticated: an orchestrator
// probes them without credentials. They are still rate limited and still carry
// the security headers.
app.use('/.well-known/*', async (c) => c.text('OK', 200));
app.get('/health', async (c) =>
  c.json({ status: 'ok', service: 'api', timestamp: new Date().toISOString() })
);
// Unauthenticated on purpose: a scraper is not a dashboard user, and the
// counters expose no telemetry, only rates and durations. Do not publish this
// port outside the compose network.
app.get('/metrics', async (c) => {
  c.header('Content-Type', registry.contentType);
  return c.body(await registry.metrics());
});

app.get('/ready', async (c) => {
  try {
    await db.execute(sql`select 1`);
    return c.json({
      status: 'ready',
      service: 'api',
      dependencies: { database: 'ready' },
      timestamp: new Date().toISOString(),
    });
  } catch {
    return c.json(
      {
        status: 'not_ready',
        service: 'api',
        dependencies: { database: 'unavailable' },
        timestamp: new Date().toISOString(),
      },
      503
    );
  }
});

app.use('*', originGuard());
app.use(prettyJSON());
app.use('*', authMiddleware);

// Errors raised in middleware bypass the route wrapper, so the app-level
// handler gives them the same envelope.
app.onError((err, c) => errorResponse(c, err));

export const appRouter = app
  .route('/auth', authRouter)
  .route('/dashboard', dashboardRouter);

export type AppType = typeof appRouter;
