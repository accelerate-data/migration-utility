import { invoke } from '@tauri-apps/api/core';
import type {
  AppSettings,
  AppPhase,
  AppPhaseState,
  ApplyWorkspaceArgs,
  DeviceFlowResponse,
  GitHubAuthResult,
  GitHubRepo,
  GitHubUser,
  RelationshipValidationResult,
  ScopeInventoryRow,
  ScopeRefreshSummary,
  ScopeTableRef,
  TableConfigPayload,
  UsageRun,
  UsageRunDetail,
  UsageSummary,
  WorkspaceApplyJobStatus,
  WorkspaceApplyProgressEvent,
  Workspace,
} from './types';

export const githubStartDeviceFlow = () =>
  invoke<DeviceFlowResponse>('github_start_device_flow');

export const githubPollForToken = (deviceCode: string) =>
  invoke<GitHubAuthResult>('github_poll_for_token', { deviceCode });

export const githubGetUser = () =>
  invoke<GitHubUser | null>('github_get_user');

export const githubLogout = () =>
  invoke<void>('github_logout');

export const githubListRepos = (query: string, limit = 10) =>
  invoke<GitHubRepo[]>('github_list_repos', { query, limit });

export const workspaceGet = () =>
  invoke<Workspace | null>('workspace_get');

export const workspaceApplyStart = (args: ApplyWorkspaceArgs) =>
  invoke<string>('workspace_apply_start', { args });

export const workspaceApplyStatus = (jobId: string) =>
  invoke<WorkspaceApplyJobStatus>('workspace_apply_status', { jobId });

export const workspaceResetState = () =>
  invoke<void>('workspace_reset_state');

export type { WorkspaceApplyProgressEvent };

export const workspaceTestSourceConnection = (args: {
  sourceType: 'sql_server' | 'fabric_warehouse';
  sourceServer: string;
  sourcePort: number;
  sourceAuthenticationMode: 'sql_password' | 'entra_service_principal';
  sourceUsername: string;
  sourcePassword: string;
  sourceEncrypt: boolean;
  sourceTrustServerCertificate: boolean;
}) =>
  invoke<string>('workspace_test_source_connection', { args });

export const workspaceDiscoverSourceDatabases = (args: {
  sourceType: 'sql_server' | 'fabric_warehouse';
  sourceServer: string;
  sourcePort: number;
  sourceAuthenticationMode: 'sql_password' | 'entra_service_principal';
  sourceUsername: string;
  sourcePassword: string;
  sourceEncrypt: boolean;
  sourceTrustServerCertificate: boolean;
}) =>
  invoke<string[]>('workspace_discover_source_databases', { args });

export const getSettings = () =>
  invoke<AppSettings>('get_settings');

export const saveAnthropicApiKey = (apiKey: string | null) =>
  invoke<void>('save_anthropic_api_key', { apiKey });

export const saveAgentSettings = (
  preferredModel: string | null,
  effort: string | null,
) => invoke<void>('save_agent_settings', { preferredModel, effort });

export const listModels = (apiKey: string) =>
  invoke<{ id: string; displayName: string }[]>('list_models', { apiKey });

export const testApiKey = (apiKey: string) =>
  invoke<boolean>('test_api_key', { apiKey });

export const appHydratePhase = () =>
  invoke<AppPhaseState>('app_hydrate_phase');

export const appSetPhase = (appPhase: AppPhase) =>
  invoke<AppPhaseState>('app_set_phase', { appPhase });

export const setLogLevel = (level: string) =>
  invoke<void>('set_log_level', { level });

export const getLogFilePath = () =>
  invoke<string>('get_log_file_path');

export const getDataDirPath = () =>
  invoke<string>('get_data_dir_path');

export const monitorLaunchAgent = (args: { prompt: string; systemPrompt?: string }) =>
  invoke<string>('monitor_launch_agent', {
    prompt: args.prompt,
    systemPrompt: args.systemPrompt ?? null,
  });

export const usageGetSummary = () =>
  invoke<UsageSummary>('usage_get_summary');

export const usageListRuns = (limit = 50) =>
  invoke<UsageRun[]>('usage_list_runs', { limit });

export const usageGetRunDetail = (runId: string) =>
  invoke<UsageRunDetail>('usage_get_run_detail', { runId });

export const migrationListScopeInventory = (workspaceId: string) =>
  invoke<ScopeInventoryRow[]>('migration_list_scope_inventory', { workspaceId });

export const migrationAddTablesToSelection = (workspaceId: string, tables: ScopeTableRef[]) =>
  invoke<number>('migration_add_tables_to_selection', { workspaceId, tables });

export const migrationSetTableSelected = (workspaceId: string, table: ScopeTableRef, selected: boolean) =>
  invoke<void>('migration_set_table_selected', { workspaceId, table, selected });

export const migrationResetSelectedTables = (workspaceId: string) =>
  invoke<number>('migration_reset_selected_tables', { workspaceId });

export const migrationSaveTableConfig = (config: TableConfigPayload) =>
  invoke<void>('migration_save_table_config', { config });

export const migrationGetTableConfig = (selectedTableId: string) =>
  invoke<TableConfigPayload | null>('migration_get_table_config', { selectedTableId });

export const migrationApproveTableConfig = (selectedTableId: string) =>
  invoke<void>('migration_approve_table_config', { selectedTableId });

export const migrationAnalyzeTableDetails = (args: {
  workspaceId: string;
  selectedTableId: string;
  schemaName: string;
  tableName: string;
  force?: boolean;
}) =>
  invoke<TableConfigPayload>('migration_analyze_table_details', {
    workspaceId: args.workspaceId,
    selectedTableId: args.selectedTableId,
    schemaName: args.schemaName,
    tableName: args.tableName,
    force: args.force ?? false,
  });

export const migrationReconcileScopeState = (workspaceId: string) =>
  invoke<ScopeRefreshSummary>('migration_reconcile_scope_state', { workspaceId });

export const migrationValidateRelationship = (
  workspaceId: string,
  currentTableId: string,
  childColumn: string,
  parentSchema: string,
  parentTable: string,
  parentColumn: string
) =>
  invoke<RelationshipValidationResult>('migration_validate_relationship', {
    workspaceId,
    currentTableId,
    childColumn,
    parentSchema,
    parentTable,
    parentColumn,
  });
