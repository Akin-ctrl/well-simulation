import { useEffect } from 'react';
import { Link, isRouteErrorResponse, useRouteError } from 'react-router';

import { routes } from '../config/routes';

/**
 * Catch-all for URLs that match no route.
 *
 * The loader throws so the error boundary renders and the response carries a
 * 404 for the client router. This previously rendered an empty fragment with its boundary
 * commented out, so an unknown URL produced a blank white page.
 */

export async function clientLoader() {
  throw new Response('Not found', { status: 404 });
}

export default function NotFound() {
  // Unreachable: the loader always throws. Present so the route is valid.
  return null;
}

export function ErrorBoundary() {
  const error = useRouteError();
  const status = isRouteErrorResponse(error) ? error.status : 500;
  const isMissing = status === 404;
  useEffect(() => {
    document.title = `${isMissing ? 'Page not found' : 'Page error'} | Well Simulation`;
    document.getElementById('missing-title')?.focus();
  }, [isMissing]);

  return (
    <main className='twin-workspace flex min-h-dvh flex-col items-center justify-center gap-3 p-8 text-center'>
      <h1 id='missing-title' tabIndex={-1}>
        {isMissing ? 'Page not found' : 'This page could not load'}
      </h1>
      <p className='twin-muted'>Error {status}</p>
      <p className='text-fgColor-muted'>
        {isMissing
          ? 'That page does not exist.'
          : 'Something went wrong loading that page.'}
      </p>
      <Link to={routes.dashboard.overview} className='text-blue-600 hover:underline'>
        Back to the dashboard
      </Link>
    </main>
  );
}
