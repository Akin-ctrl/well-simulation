import { hc, type ClientResponse } from 'hono/client';
import type { AppType } from '@well-simulation/api/src/_app';
import type { ApiErr, ApiRes } from '@well-simulation/dto/res/response';
import { ApiError } from '@well-simulation/dto/error';
import { removeUndefined } from '@well-simulation/utils/misc';
import { SERVER_BASE_URL } from '@well-simulation/utils/configs';

const client = hc<AppType>(SERVER_BASE_URL, {
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
        /**
         * The typed client for the dashboard API.
         *
         * Wraps Hono's RPC client so every call goes through one place that unwraps the
         * response envelope and turns a failure into an ApiError carrying the status.
         * Routes depend on that status: the dashboard layout signs a user out on a 401
         * and shows an error on anything else.
         */

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
    if (isRecord(successfulBody) && 'error' in successfulBody && successfulBody.error) {
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
