/** HTTP fixture for the real Docker DNS and nginx recovery check. */
import { createServer } from 'node:http';

let ready = true;
let commands = 0;

createServer(async (request, response) => {
  const url = new URL(request.url, 'http://api');
  if (url.pathname === '/ready') {
    response.writeHead(ready ? 200 : 503);
    response.end(JSON.stringify({ ready }));
    return;
  }
  if (url.pathname === '/fixture/readiness') {
    ready = url.searchParams.get('ready') === 'true';
  }
  if (url.pathname === '/fixture/command') {
    commands += 1;
    // A lost response must not cause nginx to send the command again.
    request.socket.destroy();
    return;
  }
  if (url.pathname === '/fixture/commands') {
    response.end(JSON.stringify({ commands }));
    return;
  }
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  response.setHeader('content-type', 'application/json');
  response.end(
    JSON.stringify({
      url: request.url,
      method: request.method,
      host: request.headers.host,
      origin: request.headers.origin,
      body: Buffer.concat(chunks).toString(),
    })
  );
}).listen(3000, '0.0.0.0');
