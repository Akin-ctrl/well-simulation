import { useEffect } from 'react';
import {
  Link,
  redirect,
  useLoaderData,
  useLocation,
  useNavigation,
  useOutlet,
  useRouteError,
  type ShouldRevalidateFunctionArgs,
} from 'react-router';
import { DesktopNav } from '../domains/dashboard/layouts/sidebar';
import Header from '../domains/dashboard/layouts/header';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import { ApiError } from '@well-simulation/dto/error';

import type { Role } from '@well-simulation/dto/auth/roles';

export type DashboardUser = {
  role: Role | null;
  firstName?: string | null;
  lastName?: string | null;
  userName?: string | null;
  email?: string | null;
};

export type CurrentUserData = {
  user: DashboardUser;
};

export async function clientLoader() {
  try {
    const response = await fetchFn<CurrentUserData>(client.auth.me.$get());
    return { user: response.data.user };
  } catch (error) {
    // Only a rejected session sends the user to the login page. Redirecting on
    // any failure meant a 500 or a dropped connection logged you out, which
    // hides a server problem behind what looks like an expired session.
    if (error instanceof ApiError && error.statusCode === 401) {
      return redirect(routes.auth.login);
    }
    throw error;
  }
}

/**
 * Skip the session check on the pages' timed refreshes.
 *
 * Each page refreshes its data every 10 to 60 seconds, and a refresh reruns
 * every loader on screen, this one included. That sent `/auth/me` six times a
 * minute per open tab. The session is still checked on a full page load and
 * after any form submission, such as logging out. If it expires in between,
 * the page's own data request fails with 401 and the error page below sends
 * the user to sign in.
 */
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  defaultShouldRevalidate,
}: ShouldRevalidateFunctionArgs) {
  const isTimedRefresh = !formMethod && currentUrl.href === nextUrl.href;
  return isTimedRefresh ? false : defaultShouldRevalidate;
}

/** Say what went wrong instead of showing an empty page. */
export function ErrorBoundary() {
  const error = useRouteError();
  const status = error instanceof ApiError ? error.statusCode : undefined;
  useEffect(() => {
    document.getElementById('dashboard-error-title')?.focus();
    document.title = 'Dashboard access or loading error | Well Simulation';
  }, []);

  let title = 'Something went wrong loading this page.';
  let detail = status ? `The server answered with status ${status}.` : undefined;
  if (status === 401) {
    title = 'Your session has ended.';
    detail = 'Sign in again to keep monitoring.';
  } else if (status === 403) {
    title = 'Access denied.';
    detail =
      'Your account cannot read this resource. Ask an administrator to review your access.';
  } else if (status === 429) {
    title = 'Too many requests from this browser.';
    detail = 'Wait a minute, then reload the page.';
  }

  return (
    <main className='twin-workspace flex min-h-dvh flex-col items-center justify-center gap-3 p-8 text-center'>
      <h1 id='dashboard-error-title' tabIndex={-1}>
        {title}
      </h1>
      {detail && <p className='text-sm text-fgColor-muted'>{detail}</p>}
      {status === 401 ? (
        <Link to={routes.auth.login} className='text-blue-600 hover:underline'>
          Sign in
        </Link>
      ) : (
        <a href={routes.dashboard.overview} className='text-blue-600 hover:underline'>
          Reload the dashboard
        </a>
      )}
    </main>
  );
}

export default function DashboardLayout() {
  const outlet = useOutlet();
  const data = useLoaderData() as CurrentUserData;

  const navigation = useNavigation();
  const isNavigating = navigation.state !== 'idle';
  const location = useLocation();
  useEffect(() => {
    const main = document.getElementById('main-content');
    const view = main?.querySelector('.twin-tabs [aria-current="page"]')?.textContent;
    const heading = main?.querySelector('h1')?.textContent ?? 'Dashboard';
    document.title = `${heading}${view ? ` · ${view}` : ''} | Well Simulation`;
    const target = new URLSearchParams(location.search).has('run')
      ? (document.getElementById('projection-title') ?? main)
      : main;
    target?.focus({ preventScroll: true });
  }, [location.pathname, location.search]);

  return (
    <div className='dashboard-shell relative flex w-full min-h-dvh'>
      <a className='twin-skip' href='#main-content'>
        Skip to main content
      </a>
      <DesktopNav />
      <div className='min-h-dvh grow h-full min-w-0'>
        <Header user={data.user} />
        <div
          role='status'
          className={isNavigating ? 'twin-navigation-status' : 'sr-only'}
        >
          {isNavigating ? 'Loading the requested view…' : ''}
        </div>
        <main
          id='main-content'
          tabIndex={-1}
          aria-busy={isNavigating}
          className='container mx-auto min-w-0'
        >
          <div className='px-4 md:px-6 min-w-0 pb-5 pt-6'>{outlet}</div>
        </main>
      </div>
    </div>
  );
}
