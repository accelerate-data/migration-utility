/**
 * E2E tests for the Home surface. @home
 *
 * State is pre-seeded via localStorage before each test so Zustand's persist
 * middleware hydrates with the desired state on page load — no need to drive
 * the UI through the settings form.
 */

import { test, expect, type Page } from '@playwright/test';
import { waitForAppReady } from '../helpers/app-helpers';

const STORE_KEY = 'migration-workflow';

interface StoreOverrides {
  workspaceId?: string | null;
  migrationStatus?: 'idle' | 'running' | 'complete';
}

/** Pre-seeds the Zustand persist store in localStorage before the page loads. */
async function seedStore(page: Page, overrides: StoreOverrides = {}) {
  await page.addInitScript(
    ({ key, state }) => {
      localStorage.setItem(key, JSON.stringify({ state, version: 0 }));
    },
    {
      key: STORE_KEY,
      state: {
        workspaceId: 'acme-corp',
        migrationStatus: 'idle',
        scopeStepStatus: {},
        scopeStepSavedAt: {},
        currentSurface: 'home',
        currentScopeStep: 'scope',
        selectedTableIds: [],
        ...overrides,
      },
    },
  );
}

// ── Dashboard state ───────────────────────────────────────────────────────────

test.describe('Home — dashboard state @home', () => {
  test.beforeEach(async ({ page }) => {
    await seedStore(page);
  });

  test('root redirect always lands on /home', async ({ page }) => {
    await page.goto('/');
    await waitForAppReady(page);

    await expect(page).toHaveURL(/\/home/);
    await expect(page.getByTestId('home-dashboard-state')).toBeVisible();
  });

  test('shows dashboard', async ({ page }) => {
    await page.goto('/home');
    await waitForAppReady(page);

    await expect(page.getByTestId('home-dashboard-state')).toBeVisible();
  });

  test('dashboard shows Active Migration section', async ({ page }) => {
    await page.goto('/home');
    await waitForAppReady(page);

    await expect(page.getByText('Active Migration', { exact: false })).toBeVisible();
    await expect(page.getByText('acme-corp')).toBeVisible();
  });

  test('dashboard shows Quick Actions', async ({ page }) => {
    await page.goto('/home');
    await waitForAppReady(page);

    await expect(page.getByTestId('btn-cancel-migration')).toBeVisible();
  });
});

// ── Running state ─────────────────────────────────────────────────────────────

test.describe('Home — running state @home', () => {
  test('shows "Pipeline running" badge when migration is running', async ({ page }) => {
    await seedStore(page, { workspaceId: 'acme-corp', migrationStatus: 'running' });
    await page.goto('/home');
    await waitForAppReady(page);

    await expect(page.getByText('Pipeline running')).toBeVisible();
  });

  test('does not show "Pipeline running" badge when migration is idle', async ({ page }) => {
    await seedStore(page, { workspaceId: 'acme-corp', migrationStatus: 'idle' });
    await page.goto('/home');
    await waitForAppReady(page);

    await expect(page.getByText('Pipeline running')).not.toBeVisible();
  });
});
