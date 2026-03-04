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
  private: boolean;
}

export interface ApplyWorkspaceArgs {
  name: string;
  migrationRepoName: string;
  migrationRepoPath: string;
  fabricUrl?: string | null;
  fabricServicePrincipalId?: string | null;
  fabricServicePrincipalSecret?: string | null;
  sourceType?: 'sql_server' | 'fabric_warehouse' | null;
  sourceServer?: string | null;
  sourceDatabase?: string | null;
  sourcePort?: number | null;
  sourceAuthenticationMode?: 'sql_password' | 'entra_service_principal' | null;
  sourceUsername?: string | null;
  sourcePassword?: string | null;
  sourceEncrypt?: boolean | null;
  sourceTrustServerCertificate?: boolean | null;
}

export interface WorkspaceApplyProgressEvent {
  stage:
    | 'validating_source_access'
    | 'verifying_repo'
    | 'importing_schemas'
    | 'importing_tables'
    | 'importing_procedures'
    | 'persisting_workspace'
    | 'importing_source_metadata'
    | 'completed';
  percent: number;
  message: string;
}

export interface WorkspaceApplyJobStatus {
  jobId: string;
  state: 'running' | 'succeeded' | 'failed' | 'cancelled';
  isAlive: boolean;
  stage: string | null;
  percent: number;
  message: string | null;
  error: string | null;
}

/** Workspace shape safe to receive from the backend — omits sourcePassword and fabricServicePrincipalSecret. */
export interface WorkspacePublic {
  id: string;
  displayName: string;
  migrationRepoName?: string | null;
  migrationRepoPath: string;
  fabricUrl?: string | null;
  fabricServicePrincipalId?: string | null;
  sourceType?: 'sql_server' | 'fabric_warehouse' | null;
  sourceServer?: string | null;
  sourceDatabase?: string | null;
  sourcePort?: number | null;
  sourceAuthenticationMode?: 'sql_password' | 'entra_service_principal' | null;
  sourceUsername?: string | null;
  sourceEncrypt?: boolean | null;
  sourceTrustServerCertificate?: boolean | null;
  createdAt: string;
}

/** App settings safe to receive from the backend — secrets replaced with presence booleans. */
export interface AppSettingsPublic {
  hasAnthropicKey: boolean;
  hasGithubAuth: boolean;
  githubUserLogin: string | null;
  githubUserAvatar: string | null;
  githubUserEmail: string | null;
  preferredModel: string | null;
  effort: string | null;
  logLevel: string | null;
}

export type AppPhase =
  | 'setup_required'
  | 'scope_editable'
  | 'plan_editable'
  | 'ready_to_run'
  | 'running_locked';

export interface AppPhaseState {
  appPhase: AppPhase;
  hasGithubAuth: boolean;
  hasAnthropicKey: boolean;
  isSourceApplied: boolean;
}

export type GitHubAuthResult =
  | { status: 'pending' }
  | { status: 'slow_down' }
  | { status: 'success'; user: GitHubUser };

export interface UsageSummary {
  totalRuns: number;
  completedRuns: number;
  failedRuns: number;
  totalCostUsd: number;
  totalInputTokens: number;
  totalOutputTokens: number;
}

export interface UsageRun {
  runId: string;
  transcriptPath: string;
  startedAt: string;
  completedAt: string | null;
  status: string;
  model: string;
  totalCostUsd: number;
  inputTokens: number;
  outputTokens: number;
  toolsUsed: string[];
  skillsLoaded: string[];
  preview: string;
}

export interface UsageEvent {
  eventType: string;
  label: string;
  content: string;
  timestampMs: number | null;
}

export interface UsageRunDetail {
  run: UsageRun;
  events: UsageEvent[];
}

