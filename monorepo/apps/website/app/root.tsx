import React from 'react';
import { Toaster } from '@well-simulation/ui/toast';
import { CheckIcon, XIcon } from 'lucide-react';
import globalStylsheet from './styles/global.css?url';
import logoUrl from './assets/svg/brand/logo.svg?url';
import {
  Links,
  Meta,
  Scripts,
  ScrollRestoration,
  isRouteErrorResponse,
  useOutlet,
  useRouteError,
} from 'react-router';
import type { MetaFunction } from 'react-router';

export const meta: MetaFunction = () => [
  {
    title: 'Well Simulation',
    description: 'Wellhead monitoring simulation',
  },
];

export const links = () => [
  { rel: 'stylesheet', href: globalStylsheet },
  // Without an icon link the browser requests /favicon.ico, which 404s.
  { rel: 'icon', type: 'image/svg+xml', href: logoUrl },
];

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang='en'>
      <head>
        <meta charSet='utf-8' />
        <meta name='viewport' content='width=device-width, initial-scale=1' />
        <Meta />
        <Links />
      </head>
      <body className='font-sans w-full'>
        {children}
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

export default function App() {
  const outlet = useOutlet();

  return (
    <>
      <Toaster
        position='bottom-right'
        toastOptions={{
          classNames: {
            title: 'ml-5 !text-sans',
            description: '!text-white/80 !text-sans',
            success: '!text-white',
            loader: '!text-white fill-white stroke-white',
            error: '!text-red-600',
            warning: '!text-white/70',
          },
        }}
        icons={{
          success: <CheckIcon className='size-5 stroke-2' />,
          error: <XIcon className='size-5 stroke-2' />,
        }}
      />
      {outlet}
    </>
  );
}

/**
 * Shown while the dashboard's JavaScript loads and its first data request runs.
 *
 * Without it, React Router prerenders a default page that also carries an
 * inline console hint for developers, one more script the CSP would have to
 * allow.
 */
export function HydrateFallback() {
  return (
    <main className='flex min-h-screen items-center justify-center text-sm text-fgColor-muted'>
      <h1 role='status'>Loading the dashboard</h1>
    </main>
  );
}

/**
 * The last resort for an error no route handled.
 *
 * It previously rendered an empty fragment, so any unexpected error left a
 * blank white page with nothing to say what happened.
 */
export function ErrorBoundary() {
  const error = useRouteError();
  const status = isRouteErrorResponse(error) ? error.status : undefined;

  return (
    <main className='flex min-h-dvh flex-col items-center justify-center gap-3 p-8 text-center'>
      <h1 className='text-lg font-semibold'>The dashboard could not load.</h1>
      {status && (
        <p className='text-sm text-fgColor-muted'>
          The server answered with status {status}.
        </p>
      )}
      <a href='/' className='text-blue-600 hover:underline'>
        Reload the dashboard
      </a>
    </main>
  );
}
