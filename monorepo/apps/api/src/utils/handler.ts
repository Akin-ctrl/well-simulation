import type { ApiErr, ApiRes } from '@corsight/dto/res/response';
import type { Context } from 'hono';
import { HTTPException } from 'hono/http-exception';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

/**
 * Response and error shaping for the API.
 *
 * Deliberately small. The previous version carried error handling inherited
 * from an unrelated codebase: it matched the substrings 'purchase' and
 * 'transaction' to produce a "Transaction failed." message, and unwrapped axios
 * error envelopes in a service that makes no outbound HTTP calls. A genuine
 * Postgres transaction error was rewritten into a misleading user-facing
 * message by that logic.
 */

/** Postgres unique-constraint violation. */
const UNIQUE_VIOLATION = '23505';

type DatabaseError = { code?: unknown; cause?: unknown };

/**
 * Strip bound query parameters from a database error message.
 *
 * Drizzle formats failures as `Failed query: <sql>\nparams: <values>`, so
 * logging the message verbatim writes every bound value to stdout, including
 * the bcrypt hash on a failed user insert. Standard 13 requires sensitive
 * values be redacted from logs; the SQL is kept because it is diagnostic.
 */
function redact(message: string): string {
  const paramsAt = message.indexOf('\nparams:');
  return paramsAt === -1
    ? message
    : `${message.slice(0, paramsAt)}\nparams: [redacted]`;
}

function logError(
  event: string,
  error: unknown,
  context: Record<string, unknown> = {}
) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(
    JSON.stringify({
      level: 'error',
      service: 'api',
      event,
      error: redact(message),
      code: databaseErrorCode(error),
      stack: error instanceof Error ? redact(error.stack ?? '') : undefined,
      ...context,
    })
  );
}

function serverResponse<T>(
  data: T,
  msg: string,
  status?: ContentfulStatusCode,
  meta?: { limit: number; page: number; total: number }
): ApiRes<T> {
  return { msg, data, status, meta };
}

/**
 * Find a Postgres error code, following the cause chain.
 *
 * Drizzle wraps driver failures in its own error, so the `code` that says a
 * unique constraint was violated is on `error.cause`, not the error itself.
 */
function databaseErrorCode(error: unknown, depth = 0): string | undefined {
  if (typeof error !== 'object' || error === null || depth > 5) return undefined;

  const code = (error as DatabaseError).code;
  if (typeof code === 'string') return code;

  return databaseErrorCode((error as DatabaseError).cause, depth + 1);
}

function isZodError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'ZodError'
  );
}

/**
 * Map a thrown value to a status and a safe client-facing message.
 *
 * Nothing derived from a database or runtime error reaches the client: those
 * messages leak schema detail. The full error is logged server-side instead.
 */
function classify(error: unknown): { status: ContentfulStatusCode; msg: string } {
  if (error instanceof HTTPException) {
    return { status: error.status, msg: error.message };
  }

  if (isZodError(error)) {
    return { status: 400, msg: 'Request validation failed' };
  }

  if (databaseErrorCode(error) === UNIQUE_VIOLATION) {
    // Which column collided is not disclosed: on user creation that would let a
    // caller probe whether an address or username is already registered.
    return { status: 409, msg: 'That record already exists' };
  }

  return { status: 500, msg: 'An unexpected error occurred' };
}

/**
 * Render any thrown value as the standard error envelope.
 *
 * Used by both the route wrapper and the app-level error handler, so an error
 * raised in middleware has the same shape as one raised in a handler. Without
 * this, a 403 from the capability guard came back as plain text while every
 * other error was JSON, and a client parsing the body hit a syntax error.
 */
export function errorResponse(c: Context, error: unknown): Response {
  const { status, msg } = classify(error);

  if (status >= 500) {
    logError('request_failed', error, {
      path: new URL(c.req.url).pathname,
      method: c.req.method,
    });
  }

  const body: ApiErr = { msg, error: msg, status };
  return c.json(body, status);
}

export function responseHandler<T, C extends Context>(
  fn: (c: C) => T | Promise<T>,
  msg?: string,
  code?: ContentfulStatusCode,
  meta?: { limit: number; page: number; total: number }
) {
  return async (c: C) => {
    try {
      const data = await fn(c);
      return c.json(
        serverResponse(data, msg ?? 'Request successful', code ?? 200, meta)
      );
    } catch (err) {
      return errorResponse(c, err);
    }
  };
}
