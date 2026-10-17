import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';

test('login, guide and fleet pages support learning, recovery and narrow screens', async ({
  page,
}) => {
  test.setTimeout(360_000);
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  let nextRead = 0;
  await page.route('**/api/**', async (route) => {
    const delay = Math.max(0, nextRead - Date.now());
    nextRead = Date.now() + delay + 950;
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    await route.fallback();
  });
  async function scan() {
    const result = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
      .analyze();
    expect(
      result.violations.map((v) => ({
        id: v.id,
        targets: v.nodes.map((n) => n.target),
      }))
    ).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)
    ).toBe(true);
  }
  await page.goto('/login');
  await page.setViewportSize({ width: 320, height: 900 });
  await scan();
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.locator('#login-errors')).toBeFocused();
  await page.locator('#login-errors a').first().click();
  await expect(page.getByLabel('Email or username')).toBeFocused();
  await page.getByLabel('Email or username').fill('demo@example.com');
  await page.getByLabel('Password', { exact: true }).fill('demo-operator-2026');
  await page.getByRole('button', { name: 'Show password', exact: true }).click();
  await expect(page.getByLabel('Password', { exact: true })).toHaveAttribute(
    'type',
    'text'
  );
  await page.getByRole('button', { name: 'Hide password', exact: true }).click();
  await expect(page.getByLabel('Password', { exact: true })).toHaveAttribute(
    'autocomplete',
    'current-password'
  );
  await page.route('**/api/auth/login', (route) =>
    route.fulfill({ status: 503, json: { success: false, msg: 'Unavailable' } })
  );
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.locator('#login-error')).toBeFocused();
  await expect(page.getByLabel('Email or username')).toHaveValue('demo@example.com');
  await scan();
  await page.unroute('**/api/auth/login');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Wellhead overview' })).toBeVisible();
  await expect(
    page
      .getByRole('navigation', { name: 'Main navigation' })
      .getByRole('link', { name: 'Guide', exact: true })
  ).toBeVisible();
  await page.getByText('New here? Start with one well', { exact: true }).click();
  await page.getByRole('link', { name: 'Read the guide', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Guide', exact: true })).toBeVisible();
  await expect(page.locator('#main-content')).toBeFocused();
  await scan();
  await page.getByText('How to interpret model results', { exact: true }).focus();
  await page.keyboard.press('Enter');
  await expect(
    page.getByText('The model represents a synthetic reference well.', { exact: false })
  ).toBeVisible();
  for (const [path, heading, endpoint] of [
    ['/alarms', 'Alarm Center', '**/api/dashboard/active-alarms'],
    ['/analytics', 'Analytics', '**/api/dashboard/analytics'],
  ]) {
    await page.goto(path!);
    await expect(
      page.getByRole('heading', { name: heading!, exact: true })
    ).toBeVisible();
    await scan();
    await page.route(endpoint!, (route) =>
      route.fulfill({ status: 503, json: { success: false, msg: 'Unavailable' } })
    );
    await page.getByRole('button', { name: 'Refresh view', exact: true }).click();
    await expect(page.getByText('Refresh failed.', { exact: false })).toBeVisible();
    await expect(
      page.getByRole('heading', { name: heading!, exact: true })
    ).toBeVisible();
    await scan();
    await page.unroute(endpoint!);
    await page.getByRole('button', { name: 'Try again', exact: true }).click();
    await expect(page.getByText('Refresh failed.', { exact: false })).not.toBeVisible();
  }
  await page.goto('/wellheads/1/runs?kind=scenario&offset=20');
  await expect(page.getByText('Page 2', { exact: true })).toBeVisible();
  await page.route('**/api/twin/wellheads/1/runs?*', (route) =>
    route.fulfill({ status: 503, json: { success: false, msg: 'Unavailable' } })
  );
  await page.getByRole('button', { name: 'Refresh history' }).click();
  await expect(
    page.getByText('History refresh failed.', { exact: false })
  ).toBeVisible();
  await expect(page.getByText('Page 2', { exact: true })).toBeVisible();
  await page.unroute('**/api/twin/wellheads/1/runs?*');
  await page.route('**/api/twin/wellheads/1/runs?*', (route) =>
    route.fulfill({ status: 403, json: { success: false, msg: 'Forbidden' } })
  );
  await page.getByRole('button', { name: 'Refresh history' }).click();
  await expect(page.getByRole('heading', { name: 'Access denied.' })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Saved run history' })).toHaveCount(0);
  await page.unroute('**/api/twin/wellheads/1/runs?*');
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 1000 });
    for (const path of ['/help', '/alarms', '/analytics']) {
      await page.goto(path);
      await expect(page.locator('#main-content')).toBeVisible();
      await scan();
    }
  }
  expect(errors).toEqual([]);
});
