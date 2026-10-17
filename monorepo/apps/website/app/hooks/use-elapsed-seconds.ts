import { useEffect, useState } from 'react';

/** Age a server response without trusting the browser's wall clock. */
export function useElapsedSeconds(snapshot: unknown): number {
  const [received, setReceived] = useState(() => ({ snapshot, at: performance.now() }));
  const [now, setNow] = useState(received.at);
  if (received.snapshot !== snapshot) {
    setReceived({ snapshot, at: performance.now() });
  }
  useEffect(() => {
    const timer = window.setInterval(() => setNow(performance.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  return Math.max(0, Math.floor((now - received.at) / 1000));
}
