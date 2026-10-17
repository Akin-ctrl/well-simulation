import { expect, test } from '@playwright/test';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

test('design gallery covers every screen and applicable state at three widths', async ({
  page,
}) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  const gallery = pathToFileURL(
    resolve('../../../docs/design/milestone-9/mockups/index.html')
  );
  for (const width of [1440, 390, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    for (const screen of ['fleet', 'monitoring', 'forecast', 'scenario', 'runs']) {
      const states = ['ready', 'loading', 'empty', 'stale', 'error', 'permission'];
      if (['scenario', 'forecast'].includes(screen)) states.push('invalid');
      for (const state of states) {
        await page.goto(`${gallery}?screen=${screen}&state=${state}`);
        await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
        expect(
          await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
          `${screen}, ${state}, ${width}px`
        ).toBe(true);
        if (state === 'invalid') {
          const describedBy = await page
            .locator('[aria-invalid=true]')
            .getAttribute('aria-describedby');
          await expect(page.locator(`#${describedBy}`)).toBeVisible();
        }
      }
    }
  }
  expect(errors).toEqual([]);
});
