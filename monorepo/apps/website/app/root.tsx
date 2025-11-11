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

export const meta = [
  {
    title: 'Corsight - Your Scada, Smarter and Simpler',
    description: 'Your Scada, Smarter and Simpler',
  },
];

export const links = () => [
  { rel: 'stylesheet', href: globalStylsheet },
  { rel: 'canonical', href: 'https://corsight.com' },
  { rel: 'icon', type: 'image/x-icon', href: '/favicon.ico' },
  {
    rel: 'apple-touch-icon',
    sizes: '192x192',
    href: '/android-chrome-192x192.png',
  },
  {
    rel: 'icon',
    type: 'image/png',
    sizes: '512x512',
    href: '/android-chrome-512x512.png',
  },
  // {rel: 'manifest', href: '/manifest.json'},
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
        position='top-center'
        toastOptions={{
          classNames: {
            toast:
              '!bg-black/80 !backdrop-blur-md !border-white/20 !rounded-[14px] !drop-shadow-xs !top-[6vh] py-3 px-5 !text-sans',
            title: '!text-white ml-5 !text-sans',
            description: '!text-white/80 !text-sans',
            success: '!text-white',
            loader: '!text-white fill-white stroke-white',
            error: '!text-white/80',
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
