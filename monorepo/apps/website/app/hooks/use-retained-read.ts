import { useState } from 'react';

/** Keep the last successful read in this mounted view, never across assets or sessions. */
export function useRetainedRead<T>(incoming: T | null, scope: string): T | null {
  const [last, setLast] = useState({ incoming, scope, value: incoming });
  if (last.incoming !== incoming || last.scope !== scope) {
    const value = incoming ?? (last.scope === scope ? last.value : null);
    setLast({ incoming, scope, value });
    return value;
  }
  return last.value;
}
