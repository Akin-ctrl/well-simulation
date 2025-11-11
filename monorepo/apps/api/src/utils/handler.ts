import { ApiErr, ApiRes } from '@corsight/dto/res/response';
import { Context } from 'hono';
import { ContentfulStatusCode } from 'hono/utils/http-status';

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

function isSensitiveError(error: any) {
  if (!error) return false;
  if (typeof error === 'object') {
    if (
      error.name === 'PostgresError' ||
      error.isAxiosError ||
      error.code === 'ECONNREFUSED' ||
      error.code === 'ECONNRESET' ||
      error.code === 'ENOTFOUND' ||
      error.code === 'EAI_AGAIN' ||
      error.message?.toLowerCase().includes('database') ||
      error.message?.toLowerCase().includes('internal')
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

function isTransactionError(error: any) {
  if (!error) return false;
  if (typeof error === 'object') {
    if (
      error.message?.toLowerCase().includes('purchase') ||
      error.message?.toLowerCase().includes('transaction')
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
  error: any,
  status?: ContentfulStatusCode
): ApiErr => {
  let msg = 'Something went wrong';
  let errorDetail = '';
  let hint = undefined;

  // Postgres error detection
  if (error && typeof error === 'object' && error.name === 'PostgresError') {
    msg = 'A database error occurred';
    errorDetail = error.message || '';
    if (typeof errorDetail !== 'string') errorDetail = String(errorDetail);
    if (error.code) errorDetail += ` (code: ${error.code})`;
    hint = 'Please try again later or contact support.';
  }
  // Axios error detection
  else if (error && typeof error === 'object') {
    msg = 'A network or API error occurred';
    errorDetail =
      error?.response?.data?.error?.message ||
      error?.response?.data?.message ||
      error?.response?.data?.error ||
      error?.response?.data?.msg ||
      error?.response?.data ||
      error?.message ||
      '';
    if (typeof errorDetail !== 'string') errorDetail = String(errorDetail);
    hint = 'Please check your network connection or try again.';
  } else if (error && typeof error === 'object') {
    msg = error.message || msg;
    errorDetail = error.message || '';
    if (typeof errorDetail !== 'string') errorDetail = String(errorDetail);
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
    console.log('[TRANSACTION ERROR]', error);
    msg = 'Transaction failed.';
    hint = 'Please check your transaction details or try again.';
    errorDetail = '';
  }

  return { msg, status, error: errorDetail, ...(hint ? { hint } : {}) };
};

export const serverErrorHandler = async (
  c: Context,
  error: any,
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
      console.log('ERR', err);
      const res = serverError(err as any);
      return c.json(res, res.status ?? 500);
    }
  };
};
