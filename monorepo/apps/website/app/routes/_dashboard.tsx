import {
  Link,
  redirect,
  useLoaderData,
  useNavigation,
  useOutlet,
  useRouteError,
  type ShouldRevalidateFunctionArgs,
} from 'react-router';
import { DesktopNav } from '../domains/dashboard/layouts/sidebar';
import Header from '../domains/dashboard/layouts/header';
import { cn } from '@well-simulation/utils/cn';
import { motion } from 'motion/react';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';
import { ApiError } from '@well-simulation/dto/error';

type DashboardUser = {
  firstName?: string | null;
  lastName?: string | null;
  userName?: string | null;
  email?: string | null;
};

type CurrentUserData = {
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

  let title = 'Something went wrong loading this page.';
  let detail = status ? `The server answered with status ${status}.` : undefined;
  if (status === 401) {
    title = 'Your session has ended.';
    detail = 'Sign in again to keep monitoring.';
  } else if (status === 429) {
    title = 'Too many requests from this browser.';
    detail = 'Wait a minute, then reload the page.';
  }

  return (
    <div className='flex min-h-dvh flex-col items-center justify-center gap-3 p-8 text-center'>
      <p className='text-lg font-semibold'>{title}</p>
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
    </div>
  );
}

export default function DashboardLayout() {
  const outlet = useOutlet();
  const data = useLoaderData() as CurrentUserData;

  const navigation = useNavigation();
  const isNavigating = Boolean(navigation.location);

  return (
    <div className='relative flex w-full min-h-dvh xl:flex'>
      <DesktopNav />

      <div className='min-h-dvh grow h-full'>
        <Header user={data.user} />
        <motion.main
          className={cn(
            'ease-[cubic-bezier(0.645, 0.045, 0.355, 1)] mx-auto flex justify-center gap-4 transition-opacity duration-150',
            'container',
            isNavigating ? 'opacity-50' : 'opacity-100'
          )}
        >
          <motion.div className='px-5 grow pb-5 pt-8'>{outlet}</motion.div>
        </motion.main>
      </div>
    </div>
  );
}
