import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';
import type { WellheadDetailResponse } from '@well-simulation/dto/res/dashboard';
import type { ApiRes } from '@well-simulation/dto/res/response';

async function scan(page: Page) {
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(
    results.violations.map(({ id, nodes }) => ({
      id,
      targets: nodes.map((node) => node.target),
    }))
  ).toEqual([]);
}
async function noOverflow(page: Page) {
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
  ).toBe(true);
}

test('workspace accessibility, recovery, keyboard focus and responsive states', async ({
  page,
}) => {
  test.setTimeout(360_000);
  let nextRequestAt = 0;
  await page.route('**/api/**', async (route) => {
    const delay = Math.max(0, nextRequestAt - Date.now());
    nextRequestAt = Date.now() + delay + 950;
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    await route.fallback();
  });
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto('/login');
  await page.getByPlaceholder('Enter email or username').fill('demo@example.com');
  await page.locator('input[type=password]').fill('demo-operator-2026');
  await page.getByRole('button', { name: /sign in/i }).click();
  await expect(page.getByRole('heading', { name: 'Wellhead overview' })).toBeVisible();
  await scan(page);
  await page.getByRole('button', { name: 'Collapse navigation' }).click();
  await expect(
    page
      .getByRole('navigation', { name: 'Main navigation' })
      .getByRole('link', { name: 'Overview', exact: true })
  ).toBeVisible();
  await scan(page);
  await page.keyboard.press('Tab');
  await expect(
    page.getByRole('link', { name: 'Well Simulation overview' })
  ).toBeFocused();
  await page.getByRole('link', { name: 'Skip to main content' }).focus();
  await page.keyboard.press('Enter');
  await expect(page.locator('#main-content')).toBeFocused();
  // Exercise stale telemetry without depending on the model's update timing.
  await page.route('**/api/dashboard/wellheads/1', async (route) => {
    const response = await route.fetch();
    expect(response.ok()).toBe(true);
    const payload: ApiRes<WellheadDetailResponse> = await response.json();
    payload.data.latestReadingAt = new Date(Date.now() - 120_000).toISOString();
    payload.data.latestReadingAgeSeconds = 120;
    await route.fulfill({ response, json: payload });
  });
  await page.getByRole('link', { name: 'WH-001', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Current parameters' })).toBeVisible();
  await expect(
    page.getByText('Telemetry stale, last reading', { exact: false })
  ).toBeVisible();
  await expect(page.locator('#main-content')).toBeFocused();
  await page
    .getByText('View history values as a table', { exact: true })
    .first()
    .click();
  await expect(page.getByRole('table').first()).toBeVisible();
  await scan(page);
  await page.unroute('**/api/dashboard/wellheads/1');
  await page.getByRole('link', { name: 'Forecast', exact: true }).click();
  await scan(page);
  await page.getByLabel('Duration (minutes)').fill('5');
  await page.getByRole('button', { name: 'Run forecast', exact: true }).click();
  await expect(page).toHaveURL(/forecast\?run=/);
  await expect(page.locator('#projection-title')).toBeFocused();
  const forecastUrl = page.url();
  await page.getByText('View plotted values as a table', { exact: true }).click();
  await scan(page);
  await page.getByRole('link', { name: 'Scenarios', exact: true }).click();
  await page.getByRole('button', { name: 'Add change' }).click();
  await expect(page.getByLabel('At minute', { exact: true }).nth(1)).toBeFocused();
  await page.getByRole('button', { name: 'Remove step 2' }).click();
  await expect(page.getByLabel('At minute', { exact: true })).toBeFocused();
  await page.getByLabel('Duration (minutes)').fill('5');
  await page.getByLabel('At minute', { exact: true }).fill('0');
  await page.getByLabel('Target (%)').fill('101');
  await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
  await expect(page.locator('#scenario-errors')).toBeFocused();
  await scan(page);
  await page.locator('#scenario-errors a').first().focus();
  await page.keyboard.press('Enter');
  await expect(page.getByLabel('Target (%)')).toBeFocused();
  await page.getByLabel('Target (%)').fill('60');
  await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
  await expect(page).toHaveURL(/scenarios\?run=/);
  await expect(page.locator('#projection-title')).toBeFocused();
  const scenarioUrl = page.url();
  await scan(page);
  await page.getByLabel('Scenario name').fill('Retained accessibility draft');
  await page.route('**/api/dashboard/wellheads/1', (route) =>
    route.fulfill({ status: 503, json: { success: false, msg: 'Unavailable' } })
  );
  await page.getByRole('button', { name: 'Refresh status', exact: true }).click();
  await expect(page.getByText('Refresh failed.', { exact: false })).toBeVisible();
  await expect(page.getByLabel('Scenario name')).toHaveValue(
    'Retained accessibility draft'
  );
  await scan(page);
  await page.unroute('**/api/dashboard/wellheads/1');
  await page.getByRole('button', { name: 'Try again', exact: true }).click();
  await expect(page.getByText('Refresh failed.', { exact: false })).not.toBeVisible();
  await expect(page.getByLabel('Scenario name')).toHaveValue(
    'Retained accessibility draft'
  );
  for (const path of [
    '/',
    '/wellheads/1',
    forecastUrl,
    scenarioUrl,
    '/wellheads/1/runs',
  ]) {
    await page.goto(path);
    await expect(page.locator('#main-content h1')).toBeVisible();
    await page.setViewportSize({ width: 320, height: 800 });
    await noOverflow(page);
    await scan(page);
    await page.setViewportSize({ width: 1280, height: 1000 });
    await page.evaluate(() => (document.documentElement.style.fontSize = '200%'));
    await noOverflow(page);
    await page.evaluate(() => (document.documentElement.style.fontSize = ''));
  }
  // A failed history refresh keeps its original list and checked-at time.
  await page.route('**/api/twin/wellheads/1/runs?*', (route) =>
    route.fulfill({ status: 503, json: { success: false, msg: 'Unavailable' } })
  );
  await page.getByRole('button', { name: 'Refresh history' }).click();
  await expect(
    page.getByText('History refresh failed.', { exact: false })
  ).toBeVisible();
  await expect(page.getByRole('table')).toBeVisible();
  await scan(page);
  await page.unroute('**/api/twin/wellheads/1/runs?*');
  await page.getByRole('button', { name: 'Refresh history' }).click();
  await expect(
    page.getByText('History refresh failed.', { exact: false })
  ).not.toBeVisible();
  await page.getByLabel('Search this page').fill('A name that does not exist');
  await expect(
    page.getByText('No runs match this search on the loaded page.')
  ).toBeVisible();
  await scan(page);

  await page.goto('/wellheads/1/scenarios');
  await page.route('**/api/twin/scenarios', (route) =>
    route.fulfill({ status: 403, json: { success: false, msg: 'Forbidden' } })
  );
  await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('permission');
  await scan(page);
  await page.unroute('**/api/twin/scenarios');
  await page.route('**/api/dashboard/wellheads/1', (route) =>
    route.fulfill({ status: 403, json: { success: false, msg: 'Forbidden' } })
  );
  await page.goto('/wellheads/1');
  await expect(page.getByRole('heading', { name: 'Access denied.' })).toBeVisible();
  await scan(page);
  expect(errors).toEqual([]);
});
