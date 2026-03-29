import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import type {
  AppSettingsPublic,
  DeviceFlowResponse,
  GitHubAuthResult,
  GitHubRepo,
  GitHubUser,
  InitStepEvent,
  Project,
} from './types';

/**
 * Extract a human-readable message from a Tauri command error.
 * CommandError is serialized as { kind: string; message: string } —
 * not as a plain string — so String(err) produces "[object Object]".
 */
export function tauriErrorMessage(err: unknown): string {
  if (typeof err === 'string') return err;
  if (err && typeof err === 'object') {
    const e = err as Record<string, unknown>;
    if (typeof e.message === 'string') return e.message;
  }
  return String(err);
}

// ── GitHub Auth ───────────────────────────────────────────────────────────────

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

// ── Settings ──────────────────────────────────────────────────────────────────

export const getSettings = () =>
  invoke<AppSettingsPublic>('get_settings');

export const saveRepoSettings = (fullName: string, cloneUrl: string, parentFolder: string) =>
  invoke<void>('save_repo_settings', { fullName, cloneUrl, parentFolder });

export const githubCheckRepoEmpty = (fullName: string) =>
  invoke<boolean>('github_check_repo_empty', { fullName });

// ── App info ──────────────────────────────────────────────────────────────────

export const setLogLevel = (level: string) =>
  invoke<void>('set_log_level', { level });

export const getLogFilePath = () =>
  invoke<string>('get_log_file_path');

export const getDataDirPath = () =>
  invoke<string>('get_data_dir_path');

// ── Projects ──────────────────────────────────────────────────────────────────

export const projectList = () =>
  invoke<Project[]>('project_list');

export const projectGet = (id: string) =>
  invoke<Project>('project_get', { id });

export const projectDelete = (id: string) =>
  invoke<void>('project_delete', { id });

export const projectSetActive = (id: string) =>
  invoke<void>('project_set_active', { id });

export const projectGetActive = () =>
  invoke<Project | null>('project_get_active');

export const projectCreateFull = (
  name: string,
  technology: string,
  sourcePath: string,
  dbName: string,
  extractionDatetime: string,
) =>
  invoke<Project>('project_create_full', {
    name,
    technology,
    sourcePath,
    dbName,
    extractionDatetime,
  });

/** Only applicable for SQL Server / DacPac projects. */
export const projectDetectDatabases = (dacpacPath: string) =>
  invoke<string[]>('project_detect_databases', { dacpacPath });

export const projectInit = (id: string) =>
  invoke<void>('project_init', { id });

export const appStartupSync = () =>
  invoke<void>('app_startup_sync');

export const projectDeleteFull = (id: string) =>
  invoke<void>('project_delete_full', { id });

export const projectResetLocal = (id: string) =>
  invoke<void>('project_reset_local', { id });

/** Subscribe to init step events. Returns an unlisten function. */
export const listenProjectInitStep = (handler: (event: InitStepEvent) => void) =>
  listen<InitStepEvent>('project:init:step', (e) => handler(e.payload));
