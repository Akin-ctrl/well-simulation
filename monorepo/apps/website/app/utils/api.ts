import { hc, type ClientResponse } from 'hono/client';
import type { AppType } from '@corsight/api/src/_app';
import type { ApiErr, ApiRes } from '@corsight/dto/res/response';
import { ApiError } from '@corsight/dto/error';
import { removeUndefined } from '@corsight/utils/misc';
import { SERVER_BASE_URL } from '@corsight/utils/configs';

const isServer = typeof window === 'undefined';

const baseUrl = isServer ? SERVER_BASE_URL : '/api';

const client = hc<AppType>(baseUrl, {
  fetch: (input: any, init: any) => {
    return fetch(input, { ...init, credentials: 'include' });
  },
  async headers() {
    return removeUndefined({
      platform: 'web',
    });
  },
});

async function fetchFn<T>(
  call: Promise<ClientResponse<ApiRes<T> | ApiErr>>
): Promise<ApiRes<T>> {
  try {
    const response = await call;
    let responseBody: any = null;

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
      if (responseBody?.error?.name === 'ZodError') {
        const issues = responseBody.error.issues;
        const msg = `Validation Error: ${
          issues
            ?.map((issue: any) => `${issue.path?.join('.')}: ${issue.message}`)
            .join(', ') || 'Invalid input provided'
        }`;
        throw new ApiError(
          responseBody.error,
          msg,
          response.status,
          responseBody.data,
          issues
        );
      }
      throw new ApiError(
        responseBody?.error || responseBody, // Pass the most relevant error object
        responseBody?.msg || response.statusText || 'Server error',
        response.status,
        responseBody?.data
      );
    }

    if (responseBody?.error) {
      throw new ApiError(
        responseBody.error,
        responseBody.msg || 'API returned an error',
        response.status,
        responseBody.data
      );
    }

    return responseBody;
  } catch (err: any) {
    if (err instanceof ApiError) {
      if (err.msg?.toLowerCase()?.includes('invalid or expired token')) {
        // await client.auth.logout.$post();
      }
      throw err;
    }

    const errorMessage = err?.message ?? 'An unexpected error occurred!';
    if (errorMessage?.toLowerCase()?.includes('invalid or expired token')) {
      // state.hardLogout();
    }
    throw new ApiError(err, errorMessage);
  }
}

export { client, fetchFn };
