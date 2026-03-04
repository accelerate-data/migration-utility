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

export type GitHubAuthResult =
  | { status: 'pending' }
  | { status: 'slow_down' }
  | { status: 'success'; user: GitHubUser };

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

export type AppPhase = 'setup_required' | 'configured' | 'running_locked';

export interface AppPhaseState {
  appPhase: AppPhase;
  hasGithubAuth: boolean;
  hasAnthropicKey: boolean;
  hasProject: boolean;
}

/** A migration project (safe to return to frontend — sa_password omitted). */
export interface Project {
  id: string;
  slug: string;
  name: string;
  createdAt: string;
}

