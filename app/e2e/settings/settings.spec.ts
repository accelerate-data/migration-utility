import { test, expect, type Page } from '@playwright/test';
import { waitForAppReady } from '../helpers/app-helpers';

const STORE_KEY = 'migration-workflow';

interface StoreOverrides {
  workspaceId?: string | null;
  migrationStatus?: 'idle' | 'running' | 'complete';
}

async function seedStore(page: Page, overrides: StoreOverrides = {}) {
  await page.addInitScript(
    ({ key, state }) => {
      localStorage.setItem(key, JSON.stringify({ state, version: 0 }));
    },
    {
      key: STORE_KEY,
      state: {
        workspaceId: null,
        migrationStatus: 'idle',
        scopeStepStatus: {},
        scopeStepSavedAt: {},

        currentScopeStep: 'scope',
        selectedTableIds: [],
        ...overrides,
      },
    },
  );
}

test.describe('Settings layout alignment @settings', () => {
  test('connections panel uses constrained width and full-width cards', async ({ page }) => {
    await seedStore(page, { workspaceId: 'acme-corp' });
    await page.goto('/settings');
    await waitForAppReady(page);

    const panel = page.getByTestId('settings-panel-connections');
    await expect(panel).toBeVisible();

    const panelBox = await panel.boundingBox();
    expect(panelBox).not.toBeNull();
    const outer = page.getByTestId('settings-panel-connections-outer');
    const outerBox = await outer.boundingBox();
    expect(outerBox).not.toBeNull();
    const ratio = panelBox!.width / outerBox!.width;
    expect(ratio).toBeGreaterThan(0.55);
    expect(ratio).toBeLessThan(0.65);

    const githubCard = page.getByTestId('settings-connections-github-card');
    const cardBox = await githubCard.boundingBox();
    expect(cardBox).not.toBeNull();
    expect(Math.abs((cardBox?.width ?? 0) - panelBox!.width)).toBeLessThanOrEqual(2);
  });

  test('profile tab renders logging and directories as their own cards', async ({ page }) => {
    await seedStore(page, { workspaceId: 'acme-corp' });
    await page.goto('/settings/profile');
    await waitForAppReady(page);

    await expect(page.getByTestId('settings-profile-logging-card')).toBeVisible();
    await expect(page.getByTestId('settings-profile-directories-card')).toBeVisible();
  });

  test('workspace tab shows migration repo card and lock treatment while running', async ({ page }) => {
    await seedStore(page, { workspaceId: 'acme-corp', migrationStatus: 'running' });
    await page.goto('/settings/workspace');
    await waitForAppReady(page);

    await expect(page.getByTestId('settings-panel-workspace')).toBeVisible();
    await expect(page.getByTestId('settings-workspace-fabric-card')).toBeVisible();
    await expect(page.getByTestId('settings-workspace-repo-card')).toBeVisible();
    await expect(page.getByTestId('settings-workspace-working-dir-card')).toBeVisible();
    await expect(page.getByTestId('input-source-server')).toBeVisible();
    await expect(page.getByTestId('input-repo-name')).toBeVisible();
    await expect(page.getByTestId('input-repo-path')).toBeVisible();
    await expect(page.getByTestId('btn-pick-repo-path')).toBeVisible();
    await expect(page.getByText('Workspace is locked after apply. Reset Migration to edit settings.')).toBeVisible();
    await expect(page.getByTestId('btn-apply')).toBeDisabled();
    await expect(page.getByTestId('input-source-server')).toBeDisabled();
    await expect(page.getByTestId('input-repo-name')).toBeDisabled();
    await expect(page.getByTestId('input-repo-path')).toBeDisabled();
    await expect(page.getByTestId('btn-pick-repo-path')).toBeDisabled();
  });
});
