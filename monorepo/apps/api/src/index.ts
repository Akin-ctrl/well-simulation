import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { appRouter } from './_app';

const app = new Hono();

app.route('/', appRouter);

serve(
  {
    fetch: app.fetch,
    port: 3000,
  },
  (info) => {
    console.info(
      JSON.stringify({
        level: 'info',
        service: 'api',
        event: 'server_started',
        port: info.port,
      })
    );
  }
);
