import { expect, test, type Page } from '@playwright/test';
import { waitForAppReady } from '../helpers/app-helpers';

const STORE_KEY = 'migration-workflow';

async function seedWorkspaceState(page: Page) {
  await page.addInitScript(
    ({ key }) => {
      const SESSION_KEY = '__e2e_workspace_applied__';
      const workspace = {
        id: 'ws-e2e',
        displayName: 'Migration Workspace',
        migrationRepoName: 'acme/data-platform',
        migrationRepoPath: '/selected/path',
        sourceType: 'sql_server',
        sourceServer: 'sql.acme.local',
        sourceDatabase: 'AdventureWorks',
        sourcePort: 1433,
        sourceAuthenticationMode: 'sql_password',
        sourceUsername: 'sa',
        sourcePassword: 'secret',
        sourceEncrypt: true,
        sourceTrustServerCertificate: false,
        createdAt: '2026-03-01T00:00:00Z',
      };

      localStorage.setItem(
        key,
        JSON.stringify({
          state: {
            workspaceId: null,
            migrationStatus: 'idle',
            scopeStepStatus: {},
            scopeStepSavedAt: {},
            currentSurface: 'settings',
            currentScopeStep: 'scope',
            selectedTableIds: [],
          },
          version: 0,
        }),
      );

      (window as unknown as { __TAURI_MOCK_OVERRIDES__?: Record<string, unknown> }).__TAURI_MOCK_OVERRIDES__ =
        {
          workspace_get: () => (sessionStorage.getItem(SESSION_KEY) === 'true' ? workspace : null),
          github_list_repos: () => [{ id: 1, fullName: 'acme/data-platform', private: true }],
          workspace_test_source_connection: 'Connection successful',
          workspace_discover_source_databases: ['AdventureWorks', 'master'],
          workspace_apply_start: () => {
            sessionStorage.setItem(SESSION_KEY, 'true');
            return 'job-e2e-1';
          },
          workspace_apply_status: () => ({
            jobId: 'job-e2e-1',
            state: 'succeeded',
            isAlive: false,
            stage: 'completed',
            percent: 100,
            message: 'Apply completed.',
            error: null,
          }),
          workspace_reset_state: () => {
            sessionStorage.removeItem(SESSION_KEY);
            return undefined;
          },
        };
    },
    { key: STORE_KEY },
  );
}

async function configureWorkspaceAndPassConnectionTest(page: Page) {
  await page.getByTestId('input-source-server').fill('sql.acme.local');
  await page.getByTestId('input-source-username').fill('sa');
  await page.getByTestId('input-source-password').fill('secret');

  await page.getByTestId('btn-pick-repo-path').click();

  const repoInput = page.getByTestId('input-repo-name');
  await repoInput.click();
  await expect(repoInput.locator('option[value="acme/data-platform"]')).toHaveCount(1);
  await repoInput.selectOption('acme/data-platform');

  await page.getByTestId('btn-test-connection').click();
  await expect(page.getByTestId('workspace-test-connection-success')).toBeVisible();
}

test.describe('Workspace apply/reset flow @workspace', () => {
  test('apply locks workspace and scope can be opened', async ({ page }) => {
    await seedWorkspaceState(page);
    await page.goto('/settings/workspace');
    await waitForAppReady(page);

    await configureWorkspaceAndPassConnectionTest(page);
    await expect(page.getByTestId('btn-apply')).toBeEnabled();

    await page.getByTestId('btn-apply').click();

    await expect(page.getByTestId('workspace-apply-success')).toBeVisible();
    await expect(
      page.getByText('Workspace is locked after apply. Reset Migration to edit settings.'),
    ).toBeVisible();
    await expect(page.getByTestId('btn-apply')).toBeDisabled();
    await expect(page.getByTestId('input-source-server')).toBeDisabled();
    await expect(page.getByTestId('input-repo-name')).toBeDisabled();

    await page.goto('/scope');
    await waitForAppReady(page);
    await expect(page.getByTestId('scope-select-step')).toBeVisible();
  });

  test('reset unlocks workspace and clears local workflow workspace id', async ({ page }) => {
    await seedWorkspaceState(page);
    await page.goto('/settings/workspace');
    await waitForAppReady(page);

    await configureWorkspaceAndPassConnectionTest(page);
    await page.getByTestId('btn-apply').click();
    await expect(page.getByTestId('workspace-apply-success')).toBeVisible();

    await page.getByTestId('btn-open-reset-migration-dialog').click();
    await page.getByTestId('input-reset-confirmation').fill('RESET AdventureWorks');
    await page.getByTestId('btn-confirm-reset-migration').click();

    await expect(
      page.getByText('Workspace is locked after apply. Reset Migration to edit settings.'),
    ).toHaveCount(0);
    await expect(page.getByTestId('btn-apply')).toBeDisabled();
    await expect(page.getByTestId('input-source-server')).toBeEnabled();
    await expect(page.getByTestId('input-repo-name')).toBeEnabled();
    await expect(page.getByTestId('input-source-server')).toHaveValue('');
    await expect(page.getByTestId('input-repo-name')).toHaveValue('');

    const workspaceId = await page.evaluate((key) => {
      const raw = localStorage.getItem(key);
      if (!raw) return 'missing';
      const parsed = JSON.parse(raw) as { state?: { workspaceId?: unknown } };
      return parsed?.state?.workspaceId ?? null;
    }, STORE_KEY);
    expect(workspaceId).toBeNull();
  });
});
