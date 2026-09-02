import { Counter, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

/**
 * Prometheus metrics for the API, per ADR 0029.
 *
 * A private registry rather than the global one, so a test importing this
 * module twice does not fail on a duplicate metric name.
 */

export const registry = new Registry();

collectDefaultMetrics({ register: registry });

export const httpRequests = new Counter({
  name: 'api_http_requests_total',
  help: 'HTTP requests handled, by method, route, and status',
  labelNames: ['method', 'route', 'status'] as const,
  registers: [registry],
});

export const httpDuration = new Histogram({
  name: 'api_http_request_duration_seconds',
  help: 'Time to handle a request',
  labelNames: ['method', 'route'] as const,
  registers: [registry],
});

export const authAttempts = new Counter({
  name: 'api_auth_attempts_total',
  help: 'Authentication attempts, by outcome',
  labelNames: ['outcome'] as const,
  registers: [registry],
});

export const authorizationDenials = new Counter({
  name: 'api_authorization_denials_total',
  help: 'Requests rejected by a capability guard',
  labelNames: ['capability'] as const,
  registers: [registry],
});

export const auditWriteFailures = new Counter({
  name: 'api_audit_write_failures_total',
  help: 'Audit events that could not be written',
  registers: [registry],
});
