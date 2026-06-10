import React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from '@corsight/ui/toast';
import { CheckIcon, XIcon } from 'lucide-react';
import globalStylsheet from './styles/global.css?url';
import {
  Links,
  Meta,
  Scripts,
  ScrollRestoration,
  useOutlet,
} from 'react-router';
import type { MetaFunction } from 'react-router';

export const meta: MetaFunction = () => [
  {
    title: 'Corsight - Your Scada, Smarter and Simpler',
    description: 'Your Scada, Smarter and Simpler',
  },
];

export const links = () => [
  { rel: 'stylesheet', href: globalStylsheet },
  { rel: 'canonical', href: 'https://corsight.com' },
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

  const queryClient = new QueryClient({});

  return (
    <QueryClientProvider client={queryClient}>
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
    </QueryClientProvider>
  );
}

export function ErrorBoundary() {
  return <></>;
}
