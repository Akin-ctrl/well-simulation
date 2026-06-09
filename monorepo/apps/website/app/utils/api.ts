import { hc, type ClientResponse } from 'hono/client';
import type { AppType } from '@corsight/api/src/_app';
import type { ApiErr, ApiRes } from '@corsight/dto/res/response';
import { ApiError } from '@corsight/dto/error';
import { removeUndefined } from '@corsight/utils/misc';
import { SERVER_BASE_URL } from '@corsight/utils/configs';

const isServer = typeof window === 'undefined';

const baseUrl = isServer ? SERVER_BASE_URL : '/api';

const client = hc<AppType>(baseUrl, {
  fetch: (input: RequestInfo | URL, init?: RequestInit) => {
    return fetch(input, { ...init, credentials: 'include' });
  },
  async headers() {
    return removeUndefined({
      platform: 'web',
    });
  },
});

type ValidationIssue = {
  path?: Array<string | number>;
  message: string;
};

type ErrorResponseBody = Omit<Partial<ApiErr>, 'error'> & {
  data?: unknown;
  error?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isValidationError(error: unknown): error is { issues?: ValidationIssue[] } {
  return (
    isRecord(error) &&
    error.name === 'ZodError' &&
    (error.issues === undefined || Array.isArray(error.issues))
  );
}

function parseErrorBody(value: unknown): ErrorResponseBody {
  return isRecord(value) ? value : { error: value };
}

function getErrorMessage(error: unknown): string | undefined {
  if (error instanceof Error) {
    return error.message;
  }
  if (isRecord(error) && typeof error.message === 'string') {
    return error.message;
  }
  return undefined;
}

async function fetchFn<T>(
  call: Promise<ClientResponse<ApiRes<T> | ApiErr>>
): Promise<ApiRes<T>> {
  try {
    const response = await call;
    let responseBody: unknown = null;

    try {
      responseBody = await response.json();
    } catch (jsonParseError) {
      if (!response.ok) {
        throw new ApiError(
          jsonParseError,
          `Network or malformed response: ${response.statusText || 'Unknown'}`,
          response.status
        );
      }
      // If JSON parsing fails but response was OK, it's an unexpected malformed JSON
      throw new ApiError(
        jsonParseError,
        'Malformed JSON response from API',
        response.status
      );
    }

    if (!response.ok) {
      const errorBody = parseErrorBody(responseBody);
      if (isValidationError(errorBody.error)) {
        const issues = errorBody.error.issues;
        const msg = `Validation Error: ${
          issues
            ?.map((issue) => `${issue.path?.join('.')}: ${issue.message}`)
            .join(', ') || 'Invalid input provided'
        }`;
        throw new ApiError(
          errorBody.error,
          msg,
          response.status,
          errorBody.data,
          issues
        );
      }
      throw new ApiError(
        errorBody.error || responseBody,
        errorBody.msg || response.statusText || 'Server error',
        response.status,
        errorBody.data
      );
    }

    const successfulBody = responseBody as ApiRes<T> | ErrorResponseBody;
    if (
      isRecord(successfulBody) &&
      'error' in successfulBody &&
      successfulBody.error
    ) {
      throw new ApiError(
        successfulBody.error,
        successfulBody.msg || 'API returned an error',
        response.status,
        successfulBody.data
      );
    }

    return successfulBody as ApiRes<T>;
  } catch (err: unknown) {
    if (err instanceof ApiError) {
      throw err;
    }

    const errorMessage = getErrorMessage(err) ?? 'An unexpected error occurred!';
    throw new ApiError(err, errorMessage);
  }
}

export { client, fetchFn };
