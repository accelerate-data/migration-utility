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

export const saveRepoSettings = (fullName: string, cloneUrl: string, localPath: string) =>
  invoke<void>('save_repo_settings', { fullName, cloneUrl, localPath });

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

export const projectCreate = (name: string, saPassword: string) =>
  invoke<Project>('project_create', { name, saPassword });

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
  saPassword: string,
  dacpacPath: string,
  sqlServerVersion: string,
  customer: string,
  system: string,
  dbName: string,
  extractionDatetime: string,
) =>
  invoke<Project>('project_create_full', {
    name,
    saPassword,
    dacpacPath,
    sqlServerVersion,
    customer,
    system,
    dbName,
    extractionDatetime,
  });

export const projectInit = (id: string) =>
  invoke<void>('project_init', { id });

export const projectDeleteFull = (id: string) =>
  invoke<void>('project_delete_full', { id });

export const projectResetLocal = (id: string) =>
  invoke<void>('project_reset_local', { id });

/** Subscribe to init step events. Returns an unlisten function. */
export const listenProjectInitStep = (handler: (event: InitStepEvent) => void) =>
  listen<InitStepEvent>('project:init:step', (e) => handler(e.payload));
