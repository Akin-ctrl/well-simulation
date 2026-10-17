import { expect, test, type Page, type TestInfo } from '@playwright/test';
import { resolve } from 'node:path';

/** Screenshots are evidence from the real UI, never prototype data. */
async function capture(page: Page, testInfo: TestInfo, name: string): Promise<void> {
  const path =
    process.env.UPDATE_SCREENSHOTS === '1'
      ? resolve('../../../docs/screenshots', `${name}.png`)
      : testInfo.outputPath(`${name}.png`);
  await expect(page.locator('#main-content')).toHaveAttribute('aria-busy', 'false');
  await page.mouse.move(0, 0);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path, fullPage: true, animations: 'disabled' });
}

test('monitor, project, compare, reopen, and handle failures without changing controls', async ({
  page,
}, testInfo) => {
  // Deep links and fault cases generate more reads than a person navigating.
  // Pace real API traffic below the deployed limit without changing that limit.
  let nextRequestAt = 0;
  await page.route('**/api/**', async (route) => {
    const now = Date.now();
    const delay = Math.max(0, nextRequestAt - now);
    nextRequestAt = now + delay + 900;
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    await route.fallback();
  });
  const browserErrors: string[] = [];
  page.on('pageerror', (error) => browserErrors.push(error.message));
  let submissions = 0;
  page.on('request', (request) => {
    if (
      request.method() === 'POST' &&
      /\/api\/twin\/(scenarios|wellheads\/\d+\/forecast)$/.test(
        new URL(request.url()).pathname
      )
    )
      submissions += 1;
  });
  await page.goto('/login');
  await page.getByPlaceholder('Enter email or username').fill('demo@example.com');
  await page.locator('input[type="password"]').fill('demo-operator-2026');
  await page.getByRole('button', { name: /sign in/i }).click();
  await expect(page).toHaveURL(/\/$/);

  await test.step('fleet, monitoring, and existing views', async () => {
    await expect(page.getByRole('heading', { name: 'Your wells' })).toBeVisible();
    await expect(page.getByRole('link', { name: 'WH-001', exact: true })).toBeVisible();
    await capture(page, testInfo, 'overview');
    await page.getByRole('link', { name: 'WH-001', exact: true }).click();
    await expect(
      page.getByRole('heading', { name: 'Current parameters' })
    ).toBeVisible();
    await expect(
      page.getByRole('heading', { name: 'Installed controls' })
    ).toBeVisible();
    await capture(page, testInfo, 'wellhead-detail');
    for (const [path, heading, name] of [
      ['/analytics', 'Pressure trends', 'analytics'],
      ['/alarms', 'Alarm Center', 'alarm-center'],
    ]) {
      await page.goto(path!);
      await expect(page.getByText(heading!, { exact: false }).first()).toBeVisible();
      await capture(page, testInfo, name!);
    }
  });
  const beforeResponse = await page.request.get('/api/twin/wellheads/1/controls');
  expect(beforeResponse.ok()).toBe(true);
  const before = (await beforeResponse.json()).data.running;
  let scenarioUrl = '';

  await test.step('real forecast and immutable saved reload', async () => {
    await page.goto('/wellheads/1/forecast');
    await page.getByLabel('Duration (minutes)').fill('5');
    await page.getByLabel('Save a point every (seconds)').fill('60');
    await page.getByRole('button', { name: 'Run forecast', exact: true }).click();
    await expect(page).toHaveURL(/\/forecast\?run=/);
    await expect(page.getByText('Saved result', { exact: true })).toBeVisible();
    await page.getByText('View plotted values as a table', { exact: true }).click();
    await expect(page.getByRole('table')).toBeVisible();
    await capture(page, testInfo, 'forecast');
    const count = submissions;
    await page.reload();
    await expect(page.getByText('Saved result', { exact: true })).toBeVisible();
    expect(submissions).toBe(count);
  });

  await test.step('scenario validation, execution, and saved input schedule', async () => {
    await page.goto('/wellheads/1/scenarios');
    await page.getByLabel('Scenario name').fill('Choke comparison');
    await page.getByLabel('Scenario name').focus();
    await page.keyboard.press('Tab');
    await expect(page.getByLabel('Duration (minutes)')).toBeFocused();
    await expect(page.getByLabel('Duration (minutes)')).toHaveCSS(
      'outline-style',
      'solid'
    );
    await page.getByLabel('Duration (minutes)').fill('5');
    await page.getByLabel('At minute', { exact: true }).fill('0');
    await page.getByLabel('Target (%)').fill('101');
    const count = submissions;
    await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
    await expect(page.locator('#scenario-errors')).toContainText('100');
    await expect(page.locator('#scenario-errors')).toBeFocused();
    await expect(page.getByLabel('Target (%)')).toHaveAttribute('aria-invalid', 'true');
    expect(submissions).toBe(count);
    await page.getByLabel('Target (%)').fill('50');
    await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
    await expect(page).toHaveURL(/\/scenarios\?run=/);
    scenarioUrl = page.url();
    await expect(page.getByRole('heading', { name: 'Choke comparison' })).toBeVisible();
    await page.getByText('Original run inputs', { exact: true }).click();
    await expect(
      page.getByText('Change 1, at +0 min: Choke opening: 50%', { exact: true })
    ).toBeVisible();
    await capture(page, testInfo, 'scenario');
    await page.getByLabel('Scenario name').fill('Unsubmitted draft');
    await page.getByRole('button', { name: 'Refresh status', exact: true }).click();
    await expect(
      page.getByRole('button', { name: 'Refresh status', exact: true })
    ).toBeEnabled();
    await expect(page.getByLabel('Scenario name')).toHaveValue('Unsubmitted draft');
    await expect(page.getByRole('heading', { name: 'Choke comparison' })).toBeVisible();
  });
  const afterResponse = await page.request.get('/api/twin/wellheads/1/controls');
  expect(afterResponse.ok()).toBe(true);
  const after = (await afterResponse.json()).data.running;
  expect(after.controls).toEqual(before.controls);
  expect(after.revision).toBe(before.revision);

  await test.step('history can reopen a result without creating another run', async () => {
    const count = submissions;
    await page.getByRole('link', { name: 'Saved runs', exact: true }).click();
    await expect(
      page.getByRole('heading', { name: 'Saved runs', exact: true })
    ).toBeVisible();
    await page.getByRole('button', { name: 'Refresh history' }).click();
    await expect(
      page.getByRole('link', { name: 'Choke comparison', exact: true }).first()
    ).toBeVisible();
    await capture(page, testInfo, 'saved-runs');
    await page
      .getByRole('link', { name: 'Choke comparison', exact: true })
      .first()
      .click();
    await expect(page.getByText('Saved result', { exact: true })).toBeVisible();
    expect(submissions).toBe(count);
  });

  await test.step('mobile navigation, forms, and charts fit the viewport', async () => {
    await page.setViewportSize({ width: 390, height: 844 });
    for (const route of ['/', '/wellheads/1', scenarioUrl, '/wellheads/1/runs']) {
      await page.goto(route);
      await expect(
        page.getByRole('navigation', { name: 'Main navigation' })
      ).toBeVisible();
      const overflows = await page.evaluate(
        () => document.documentElement.scrollWidth > window.innerWidth + 1
      );
      expect(overflows, `Horizontal overflow at ${route}`).toBe(false);
      if (route === scenarioUrl) await capture(page, testInfo, 'scenario-mobile');
    }
    await page.setViewportSize({ width: 1440, height: 1000 });
  });

  await test.step('delayed refresh ages the retained status', async () => {
    await page.goto('/wellheads/1');
    await expect(
      page.getByRole('heading', { name: 'Current parameters' })
    ).toBeVisible();
    await page.clock.install();
    let release: () => void = () => {};
    let statusRequests = 0;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    await page.route('**/api/twin/status', async (route) => {
      statusRequests += 1;
      await blocked;
      await route.continue();
    });
    try {
      await page.getByRole('button', { name: 'Refresh status', exact: true }).click();
      await expect(
        page.getByRole('button', { name: 'Refreshing status…', exact: true })
      ).toBeDisabled();
      await expect.poll(() => statusRequests).toBe(1);
      await page.clock.fastForward(35000);
      await expect(
        page.getByText('Model check overdue', { exact: true }).first()
      ).toBeVisible();
      await expect(page.getByText(/Telemetry stale, last reading/)).toBeVisible();
      expect(statusRequests).toBe(1);
    } finally {
      release();
      await expect(
        page.getByRole('button', { name: 'Refresh status', exact: true })
      ).toBeEnabled();
      await page.unroute('**/api/twin/status');
    }
  });

  await test.step('unavailable model preserves the monitoring view', async () => {
    await page.route('**/api/twin/status', (route) =>
      route.fulfill({
        status: 200,
        json: {
          data: {
            checkedAt: new Date().toISOString(),
            available: false,
            wellheads: [],
          },
          status: 200,
        },
      })
    );
    await page.goto('/wellheads/1');
    await expect(
      page.getByText('Model unavailable', { exact: true }).first()
    ).toBeVisible();
    await expect(
      page.getByRole('heading', { name: 'Current parameters' })
    ).toBeVisible();
    await page.unroute('**/api/twin/status');
  });

  await test.step('expired result and wrong-well links are explicit', async () => {
    await page.route('**/api/twin/scenarios/*', (route) =>
      route.fulfill({
        status: 410,
        json: { error: 'expired', msg: 'Forecast points have expired', status: 410 },
      })
    );
    await page.goto(scenarioUrl);
    await expect(page.getByRole('alert')).toContainText('expired');
    await page.unroute('**/api/twin/scenarios/*');
    await page.goto(scenarioUrl.replace('/wellheads/1/', '/wellheads/2/'));
    await expect(page.getByRole('alert')).toContainText('could not find');
  });

  await test.step('timeout keeps the draft and offers outcome discovery without retry', async () => {
    await page.route('**/api/twin/scenarios', (route) =>
      route.fulfill({
        status: 504,
        json: { error: 'timeout', msg: 'Twin service timed out', status: 504 },
      })
    );
    await page.goto('/wellheads/1/scenarios');
    await page.getByLabel('Scenario name').fill('Retain this draft');
    const count = submissions;
    await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
    await expect(page.getByRole('alert')).toContainText('timed out');
    await expect(
      page.getByRole('link', { name: 'Check saved runs', exact: true })
    ).toBeVisible();
    await expect(page.getByLabel('Scenario name')).toHaveValue('Retain this draft');
    expect(submissions).toBe(count + 1);
    await page.unroute('**/api/twin/scenarios');

    await page.route('**/api/twin/wellheads/1/forecast', (route) =>
      route.fulfill({ status: 504, json: { error: 'timeout', status: 504 } })
    );
    await page.goto('/wellheads/1/forecast');
    await page.getByRole('button', { name: 'Run forecast', exact: true }).click();
    await expect(page.getByRole('alert')).toContainText('timed out');
    await expect(
      page.getByRole('link', { name: 'Check saved runs', exact: true })
    ).toHaveAttribute('href', '/wellheads/1/runs?kind=forecast');
    await page.unroute('**/api/twin/wellheads/1/forecast');
  });

  await test.step('permission and busy errors preserve editable inputs', async () => {
    for (const [status, message] of [
      [403, 'permission'],
      [429, 'Too many requests'],
      [503, 'busy or unavailable'],
    ] as const) {
      await page.route('**/api/twin/scenarios', (route) =>
        route.fulfill({
          status,
          json: { error: 'rejected', msg: 'Request rejected', status },
        })
      );
      await page.goto('/wellheads/1/scenarios');
      await page.getByLabel('Scenario name').fill('Keep my inputs');
      await page.getByRole('button', { name: 'Run scenario', exact: true }).click();
      await expect(page.getByRole('alert')).toContainText(message);
      await expect(page.getByLabel('Scenario name')).toHaveValue('Keep my inputs');
      await expect(
        page.getByRole('button', { name: 'Run scenario', exact: true })
      ).toBeEnabled();
      await page.unroute('**/api/twin/scenarios');
    }
  });

  await test.step('expired session has a sign-in path', async () => {
    await page.route('**/api/dashboard/wellheads/1', (route) =>
      route.fulfill({
        status: 401,
        json: { error: 'session expired', msg: 'Session expired', status: 401 },
      })
    );
    await page.goto('/wellheads/1');
    await expect(
      page.getByText('Your session has ended.', { exact: true })
    ).toBeVisible();
    await expect(
      page.getByRole('link', { name: 'Sign in', exact: true })
    ).toBeVisible();
    await page.unroute('**/api/dashboard/wellheads/1');
  });
  expect(browserErrors).toEqual([]);
});
