export interface DeviceFlowResponse {
  device_code: string;
  user_code: string;
  verification_uri: string;
  expires_in: number;
  interval: number;
}

export interface GitHubUser {
  login: string;
  avatar_url: string;
  email: string | null;
}

export interface GitHubRepo {
  id: number;
  fullName: string;
  cloneUrl: string;
  private: boolean;
}

export type GitHubAuthResult =
  | { status: 'pending' }
  | { status: 'slow_down' }
  | { status: 'success'; user: GitHubUser };

/** App settings safe to receive from the backend — secrets replaced with presence booleans. */
export interface AppSettingsPublic {
  hasGithubAuth: boolean;
  githubUserLogin: string | null;
  githubUserAvatar: string | null;
  githubUserEmail: string | null;
  logLevel: string | null;
  migrationRepoFullName: string | null;
  migrationRepoCloneUrl: string | null;
  localClonePath: string | null;
  activeProjectId: string | null;
}

/** A migration project (safe to return to frontend — sa_password omitted). */
export interface Project {
  id: string;
  slug: string;
  name: string;
  createdAt: string;
}

// ── Init orchestrator types ───────────────────────────────────────────────────

export type InitStep =
  | 'gitPull'
  | 'dockerCheck'
  | 'startContainer'
  | 'restoreDacpac'
  | 'verifyDb';

export type InitStepStatus =
  | { kind: 'running' }
  | { kind: 'ok' }
  | { kind: 'warning'; warnings: string[] }
  | { kind: 'error'; message: string };

export interface InitStepEvent {
  step: InitStep;
  status: InitStepStatus;
  /** Absent for global steps (gitPull, dockerCheck); present for per-project steps. */
  projectId?: string;
}

export const GLOBAL_STEPS: InitStep[] = ['gitPull', 'dockerCheck'];
export const PER_PROJECT_STEPS: InitStep[] = ['startContainer', 'restoreDacpac', 'verifyDb'];

export const INIT_STEPS: InitStep[] = [
  'gitPull',
  'dockerCheck',
  'startContainer',
  'restoreDacpac',
  'verifyDb',
];

export const INIT_STEP_LABEL: Record<InitStep, string> = {
  gitPull: 'Sync repository',
  dockerCheck: 'Check Docker',
  startContainer: 'Start SQL container',
  restoreDacpac: 'Restore database',
  verifyDb: 'Verify connectivity',
};

