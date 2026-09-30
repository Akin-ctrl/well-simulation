/**
 * Where the dashboard reaches the API.
 *
 * Always a relative path. nginx proxies `/api` to the API container, so the
 * browser never makes a cross-origin request and the origin guard on the API
 * has a same-origin request to check.
 *
 * This used to fall back to `https://api.corsight.com` outside development, a
 * domain the project does not own. It was unreachable dead code, because the
 * app sets `ssr: false` and therefore only ever runs in a browser, but it was
 * still a wrong absolute URL sitting in shipped config.
 */
export const SERVER_BASE_URL = '/api';
