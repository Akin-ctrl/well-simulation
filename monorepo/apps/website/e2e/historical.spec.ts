import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';

test('select an exact historical state, keep drafts through failures and reopen the saved branch', async ({
  page,
}, testInfo) => {
  let nextRead = 0;
  await page.route('**/api/**', async (route) => {
    const delay = Math.max(0, nextRead - Date.now());
    nextRead = Date.now() + delay + 900;
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    await route.fallback();
  });
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/login');
  await page.getByLabel('Email or username').fill('demo@example.com');
  await page.getByLabel('Password', { exact: true }).fill('demo-operator-2026');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page).toHaveURL(/\/$/);
  const before = (
    await (await page.request.get('/api/twin/wellheads/1/controls')).json()
  ).data.running;
  await page.goto('/wellheads/1/scenarios');
  await page.getByLabel('Scenario name').fill('Historical choke comparison');
  await page.getByLabel('Duration (minutes)').fill('5');
  await page.getByLabel('At minute', { exact: true }).fill('0');
  await page.getByLabel('Target (%)').fill('65');
  await page.getByLabel('Saved snapshot', { exact: true }).check();
  await expect(
    page.getByRole('button', { name: 'Run scenario', exact: true })
  ).toBeDisabled();
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  const candidates = page.getByRole('group', { name: 'Saved snapshots', exact: true });
  const candidate = candidates.getByRole('radio').first();
  await expect(candidate).toBeEnabled();
  await candidate.focus();
  await page.keyboard.press('Space');
  await expect(
    page.getByRole('button', { name: 'Run scenario', exact: true })
  ).toBeEnabled();
  let submitted:
    | { baseSnapshot: { snapshotId: string; simulatedAt: string } }
    | undefined;
  page.on('request', (request) => {
    if (
      request.method() === 'POST' &&
      new URL(request.url()).pathname === '/api/twin/scenarios'
    )
      submitted = request.postDataJSON();
  });
  await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
  await expect(page).toHaveURL(/scenarios\?run=/);
  await expect(page.getByText('Saved result', { exact: true })).toBeVisible();
  expect(submitted?.baseSnapshot.snapshotId).toMatch(/^[1-9][0-9]*$/);
  const runId = new URL(page.url()).searchParams.get('run');
  const saved = (await (await page.request.get(`/api/twin/scenarios/${runId}`)).json())
    .data;
  expect(saved.branchOrigin).toEqual({ kind: 'snapshot', ...submitted!.baseSnapshot });
  expect(Date.parse(saved.baseSimulatedAt)).toBe(
    Date.parse(saved.branchOrigin.simulatedAt)
  );
  await page.reload();
  await expect(page.getByText(/Starting state: Saved snapshot/)).toBeVisible();
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath('historical-result.png'),
    fullPage: true,
  });
  const after = (
    await (await page.request.get('/api/twin/wellheads/1/controls')).json()
  ).data.running;
  expect(after.controls).toEqual(before.controls);
  expect(after.revision).toBe(before.revision);

  await page.goto('/wellheads/1/scenarios');
  await page.getByLabel('Scenario name').fill('Keep this draft');
  await page.getByLabel('Saved snapshot', { exact: true }).check();
  let finderState = 'empty';
  let releaseSearch: (() => void) | undefined;
  const delayedSearch = new Promise<void>((resolve) => {
    releaseSearch = resolve;
  });
  const finder = '**/api/twin/wellheads/1/snapshots?*';
  await page.route(finder, async (route) => {
    if (finderState === 'delayed') await delayedSearch;
    await route.fulfill({
      json: {
        data: {
          wellheadId: 1,
          checkedAt: new Date().toISOString(),
          hasMore: false,
          items:
            finderState === 'empty'
              ? []
              : [
                  {
                    ...submitted!.baseSnapshot,
                    modelVersion: saved.modelVersion,
                    compatible: finderState !== 'incompatible',
                    reason:
                      finderState === 'incompatible'
                        ? 'parameters_changed'
                        : 'compatible',
                  },
                ],
        },
      },
    });
  });
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  await expect(
    page.getByText('No snapshots in this search window. Try another time.')
  ).toBeVisible();
  finderState = 'incompatible';
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  await expect(candidates.getByRole('radio')).toBeDisabled();
  finderState = 'delayed';
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Finding snapshots…', exact: true })
  ).toBeVisible();
  await page.getByLabel('Find snapshots at or before').fill('2026-10-16T12:00');
  releaseSearch!();
  await expect(candidates).toHaveCount(0);
  await expect(
    page.getByRole('button', { name: 'Run scenario', exact: true })
  ).toBeDisabled();
  finderState = 'ready';
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  await candidates.getByRole('radio').check();
  await page.getByLabel('Duration (minutes)').fill('5');
  await page.getByLabel('At minute', { exact: true }).fill('0');
  await page.route('**/api/twin/scenarios', (route) =>
    route.fulfill({ status: 409, json: { msg: 'Snapshot changed' } })
  );
  await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Run scenario', exact: true })
  ).toBeDisabled();
  await expect(candidates).toHaveCount(0);
  await expect(page.getByLabel('Scenario name')).toHaveValue('Keep this draft');
  await page.unroute('**/api/twin/scenarios');
  await page.unroute(finder);
  let failStatus = 503;
  await page.route('**/api/twin/wellheads/1/snapshots?*', (route) =>
    route.fulfill({ status: failStatus, json: { msg: 'Unavailable' } })
  );
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  await expect(
    page.getByText('The model or database is busy or unavailable.')
  ).toBeVisible();
  await expect(page.getByLabel('Scenario name')).toHaveValue('Keep this draft');
  await expect(
    page.getByRole('button', { name: 'Run scenario', exact: true })
  ).toBeDisabled();
  failStatus = 403;
  await page.getByRole('button', { name: 'Find snapshots', exact: true }).click();
  await expect(
    page.getByText('Your account does not have permission for this action.')
  ).toBeVisible();
  await expect(page.getByLabel('Current state', { exact: true })).toBeDisabled();
  await expect(
    page.getByRole('group', { name: 'Saved snapshots', exact: true })
  ).toHaveCount(0);
  await page.setViewportSize({ width: 320, height: 800 });
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
  ).toBe(true);
  expect(
    (
      await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
        .analyze()
    ).violations
  ).toEqual([]);
  expect(errors).toEqual([]);
});
