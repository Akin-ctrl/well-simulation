import { randomUUID } from 'node:crypto';
import type { MiddlewareHandler } from 'hono';

/**
 * Give every request an id, and put it on the response.
 *
 * ADR 0034 stores this id on each audit row. Without it an audit event says
 * what happened but gives you no way back to the request that caused it, which
 * is most of the value of having the row at all.
 *
 * An inbound `x-request-id` is honoured so a value set by a proxy or a caller
 * survives, but it is length-capped: it is attacker-controlled and it ends up
 * in a database column and in every log line.
 */

const HEADER = 'x-request-id';
const MAX_LENGTH = 64;
const SAFE_PATTERN = /^[A-Za-z0-9._-]+$/;

export function requestId(): MiddlewareHandler {
  return async (c, next) => {
    const supplied = c.req.header(HEADER);
    const id =
      supplied && supplied.length <= MAX_LENGTH && SAFE_PATTERN.test(supplied)
        ? supplied
        : randomUUID();

    c.set('requestId', id);
    c.header(HEADER, id);

    return next();
  };
}

/** Read the current request id. Empty only if the middleware did not run. */
export function currentRequestId(c: { get: (key: string) => unknown }): string {
  const id = c.get('requestId');
  return typeof id === 'string' ? id : '';
}
