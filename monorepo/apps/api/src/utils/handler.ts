import type { ApiErr, ApiRes } from '@corsight/dto/res/response';
import type { Context } from 'hono';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

type ErrorLike = {
  name?: string;
  code?: string;
  message?: string;
  status?: ContentfulStatusCode;
  isAxiosError?: boolean;
  response?: {
    data?: unknown;
  };
};

function logError(event: string, error: unknown) {
  const errorValue = error instanceof Error ? error.message : String(error);
  console.error(
    JSON.stringify({
      level: 'error',
      service: 'api',
      event,
      error: errorValue,
    })
  );
}

function asErrorLike(error: unknown): ErrorLike | null {
  if (!error || typeof error !== 'object') {
    return null;
  }
  return error as ErrorLike;
}

function stringifyErrorDetail(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }
  if (value === undefined || value === null) {
    return '';
  }
  return String(value);
}

function readResponseMessage(data: unknown): string {
  if (!data || typeof data !== 'object') {
    return stringifyErrorDetail(data);
  }
  const responseData = data as Record<string, unknown>;
  const nestedError = responseData.error;
  if (nestedError && typeof nestedError === 'object') {
    const nested = nestedError as Record<string, unknown>;
    if (typeof nested.message === 'string') return nested.message;
  }
  for (const key of ['message', 'error', 'msg']) {
    if (typeof responseData[key] === 'string') {
      return responseData[key];
    }
  }
  return stringifyErrorDetail(data);
}

export const serverResponse = <T>(
  data: T,
  msg: string,
  status?: ContentfulStatusCode,
  meta?: {
    limit: number;
    page: number;
    total: number;
  }
): ApiRes<T> => {
  return { msg, data, status, meta };
};

export const serverResponseHandler = async <T>(
  c: Context,
  data: T,
  msg: string,
  code?: ContentfulStatusCode,
  meta?: {
    limit: number;
    page: number;
    total: number;
  }
) => {
  return c.json(
    serverResponse(data, msg, code, meta) as ApiRes<T>,
    code ?? 200
  );
};

function isSensitiveError(error: unknown) {
  if (!error) return false;
  const errorLike = asErrorLike(error);
  if (errorLike) {
    if (
      errorLike.name === 'PostgresError' ||
      errorLike.isAxiosError ||
      errorLike.code === 'ECONNREFUSED' ||
      errorLike.code === 'ECONNRESET' ||
      errorLike.code === 'ENOTFOUND' ||
      errorLike.code === 'EAI_AGAIN' ||
      errorLike.message?.toLowerCase().includes('database') ||
      errorLike.message?.toLowerCase().includes('internal')
    ) {
      return true;
    }
  }
  if (typeof error === 'string') {
    if (
      error.toLowerCase().includes('database') ||
      error.toLowerCase().includes('internal')
    ) {
      return true;
    }
  }
  return false;
}

function isTransactionError(error: unknown) {
  if (!error) return false;
  const errorLike = asErrorLike(error);
  if (errorLike) {
    if (
      errorLike.message?.toLowerCase().includes('purchase') ||
      errorLike.message?.toLowerCase().includes('transaction')
    ) {
      return true;
    }
  }
  if (typeof error === 'string') {
    if (
      error.toLowerCase().includes('purchase') ||
      error.toLowerCase().includes('transaction')
    ) {
      return true;
    }
  }
  return false;
}

export const serverError = (
  error: unknown,
  status?: ContentfulStatusCode
): ApiErr => {
  let msg = 'Something went wrong';
  let errorDetail = '';
  let hint = undefined;
  const errorLike = asErrorLike(error);
  const effectiveStatus = status ?? errorLike?.status;

  if (errorLike?.name === 'PostgresError') {
    msg = 'A database error occurred';
    errorDetail = errorLike.message || '';
    if (errorLike.code) errorDetail += ` (code: ${errorLike.code})`;
    hint = 'Please try again later or contact support.';
  } else if (errorLike?.status) {
    msg = errorLike.message || msg;
  } else if (errorLike) {
    msg = 'A network or API error occurred';
    errorDetail =
      readResponseMessage(errorLike.response?.data) || errorLike.message || '';
    hint = 'Please check your network connection or try again.';
  } else if (typeof error === 'string') {
    msg = error;
    errorDetail = error;
  }

  if (isSensitiveError(error)) {
    errorDetail = '';
    msg = 'An unexpected error occurred.';
    hint = 'Please try again later or contact support.';
  }

  if (isTransactionError(error)) {
    logError('transaction_error', error);
    msg = 'Transaction failed.';
    hint = 'Please check your transaction details or try again.';
    errorDetail = '';
  }

  return {
    msg,
    status: effectiveStatus,
    error: errorDetail,
    ...(hint ? { hint } : {}),
  };
};

export const serverErrorHandler = async (
  c: Context,
  error: unknown,
  code: ContentfulStatusCode | undefined
) => {
  return c.json(serverError(error, code), code ?? 500);
};

export const responseHandler = <T, C extends Context>(
  fn: (c: C) => T | Promise<T>,
  msg?: string,
  code?: ContentfulStatusCode,
  meta?: {
    limit: number;
    page: number;
    total: number;
  }
) => {
  return async (c: C) => {
    try {
      return c.json(
        serverResponse(
          await fn(c),
          msg ?? 'Request successful',
          code ?? 200,
          meta
        )
      );
    } catch (err) {
      logError('request_handler_error', err);
      const res = serverError(err);
      return c.json(res, res.status ?? 500);
    }
  };
};
