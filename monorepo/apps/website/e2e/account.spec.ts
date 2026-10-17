import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';

test('account access, administrator form outcomes and missing-page recovery', async ({
  page,
}) => {
  await page.setViewportSize({ width: 320, height: 900 });
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
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
  await page.goto('/unknown-page');
  await expect(page.getByRole('heading', { name: 'Page not found' })).toBeFocused();
  await scan();
  await page.goto('/register');
  await expect(
    page.getByText('An administrator creates dashboard accounts.', { exact: false })
  ).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Create account', exact: true })
  ).toHaveCount(0);
  await scan();
  // Mock the administrator presentation only. Real API authorization has its own suite.
  await page.route('**/api/auth/me', (route) =>
    route.fulfill({
      status: 200,
      json: {
        data: { user: { role: 'ADMIN', userName: 'Test administrator' } },
        status: 200,
        msg: 'Current user',
      },
    })
  );
  await page.goto('/register');
  await page.getByRole('button', { name: 'Create account', exact: true }).click();
  await expect(page.getByLabel('Username', { exact: true })).toBeFocused();
  await scan();
  await page.getByLabel('Username', { exact: true }).fill('new-reader');
  await page.getByLabel('Email', { exact: true }).fill('reader@example.com');
  await page.getByLabel('Password', { exact: true }).fill('local-test-password-2026');
  await page
    .getByLabel('Confirm password', { exact: true })
    .fill('local-test-password-2026');
  let writes = 0;
  await page.route('**/api/auth/register', (route) => {
    writes++;
    return route.fulfill({ status: 504, json: { msg: 'Unavailable' } });
  });
  await page.getByRole('button', { name: 'Create account', exact: true }).click();
  await expect(page.locator('#account-result')).toContainText(
    'Check existing accounts'
  );
  await expect(page.locator('#account-result')).toBeFocused();
  expect(writes).toBe(1);
  await page.unroute('**/api/auth/register');
  await page.route('**/api/auth/register', (route) => {
    writes++;
    expect(route.request().postDataJSON()).toMatchObject({
      username: 'new-reader',
      email: 'reader@example.com',
    });
    expect(route.request().postDataJSON().role).toBeUndefined();
    return route.fulfill({
      status: 200,
      json: {
        data: { user: { userName: 'new-reader', role: 'USER' } },
        status: 200,
        msg: 'Created',
      },
    });
  });
  await page.getByRole('button', { name: 'Create account', exact: true }).click();
  await expect(page.locator('#account-result')).toContainText(
    'Account created for new-reader.'
  );
  await expect(page.getByLabel('Password', { exact: true })).toHaveValue('');
  expect(writes).toBe(2);
  await scan();
  expect(errors).toEqual([]);
});
