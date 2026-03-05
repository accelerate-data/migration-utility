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
}

/** A migration project (safe to return to frontend — sa_password omitted). */
export interface Project {
  id: string;
  slug: string;
  name: string;
  createdAt: string;
}

