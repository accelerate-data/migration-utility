import { invoke } from '@tauri-apps/api/core';
import type {
  AppSettingsPublic,
  DeviceFlowResponse,
  GitHubAuthResult,
  GitHubRepo,
  GitHubUser,
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
