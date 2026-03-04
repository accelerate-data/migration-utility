import { invoke } from '@tauri-apps/api/core';
import type {
  AppPhaseState,
  ApplyWorkspaceArgs,
  DeviceFlowResponse,
  GitHubAuthResult,
  GitHubRepo,
  GitHubUser,
  UsageRun,
  UsageRunDetail,
  UsageSummary,
  WorkspaceApplyJobStatus,
  WorkspacePublic,
  AppSettingsPublic,
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
  invoke<WorkspacePublic | null>('workspace_get');

export const workspaceApplyStart = (args: ApplyWorkspaceArgs) =>
  invoke<string>('workspace_apply_start', { args });

export const workspaceApplyStatus = (jobId: string) =>
  invoke<WorkspaceApplyJobStatus>('workspace_apply_status', { jobId });

export const workspaceResetState = () =>
  invoke<void>('workspace_reset_state');

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
  invoke<AppSettingsPublic>('get_settings');

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

export const setLogLevel = (level: string) =>
  invoke<void>('set_log_level', { level });

export const getLogFilePath = () =>
  invoke<string>('get_log_file_path');

export const getDataDirPath = () =>
  invoke<string>('get_data_dir_path');

export const usageGetSummary = () =>
  invoke<UsageSummary>('usage_get_summary');

export const usageListRuns = (limit = 50) =>
  invoke<UsageRun[]>('usage_list_runs', { limit });

export const usageGetRunDetail = (runId: string) =>
  invoke<UsageRunDetail>('usage_get_run_detail', { runId });
