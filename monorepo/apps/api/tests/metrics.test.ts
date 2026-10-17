import { afterEach, expect, it, vi } from 'vitest';
import { db } from '@well-simulation/db/query';
import { databaseReady, readinessCheckedAt } from '../src/metrics';
import { request } from './helpers/api';

afterEach(() => vi.restoreAllMocks());

it('reports the last completed readiness probe without probing during a scrape', async () => {
  databaseReady.set(0);
  readinessCheckedAt.set(0);
  const execute = vi.spyOn(db, 'execute');
  expect((await request('/ready')).status).toBe(200);
  const first = await (await request('/metrics')).text();
  expect(first).toContain('well_sim_api_database_ready 1');
  expect(execute).toHaveBeenCalledTimes(1);
  const checked = (await readinessCheckedAt.get()).values[0]?.value;
  expect(checked).toBeGreaterThan(Date.now() / 1000 - 5);

  execute.mockRejectedValueOnce(new Error('database unavailable'));
  expect((await request('/ready')).status).toBe(503);
  const failed = await (await request('/metrics')).text();
  expect(failed).toContain('well_sim_api_database_ready 0');
  expect(execute).toHaveBeenCalledTimes(2);
  expect(failed).not.toContain('database unavailable');
});
