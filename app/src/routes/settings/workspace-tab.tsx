import { useEffect, useRef, useState } from 'react';
import { open as openDialog } from '@tauri-apps/plugin-dialog';
import { Lock } from 'lucide-react';
import { useWorkflowStore } from '@/stores/workflow-store';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import {
  appHydratePhase,
  workspaceDiscoverSourceDatabases,
  githubListRepos,
  workspaceApplyStart,
  workspaceApplyStatus,
  workspaceGet,
  workspaceResetState,
  workspaceTestSourceConnection,
} from '@/lib/tauri';
import type { GitHubRepo } from '@/lib/types';
import { logger } from '@/lib/logger';

const DEFAULT_WORKSPACE_NAME = 'Migration Workspace';

type SourceType = 'sql_server';
type SourceAuthenticationMode = 'sql_password' | 'entra_service_principal';

const SOURCE_DEFAULTS: {
  port: number;
  authenticationMode: SourceAuthenticationMode;
  encrypt: boolean;
  trustServerCertificate: boolean;
} = {
  port: 1433,
  authenticationMode: 'sql_password',
  encrypt: true,
  trustServerCertificate: false,
};

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  if (err && typeof err === 'object') {
    const maybe = err as { message?: unknown; kind?: unknown };
    if (typeof maybe.message === 'string' && maybe.message.trim()) {
      return maybe.message;
    }
    if (typeof maybe.kind === 'string' && maybe.kind.trim()) {
      return maybe.kind;
    }
    try {
      return JSON.stringify(err);
    } catch {
      return 'Unknown error';
    }
  }
  return String(err);
}

export default function WorkspaceTab() {
  const { setWorkspaceId, clearWorkspaceId, appPhase, setAppPhaseState, reset } = useWorkflowStore();
  const isLocked = appPhase === 'running_locked';

  const [workspaceName, setWorkspaceName] = useState(DEFAULT_WORKSPACE_NAME);
  const [repoName, setRepoName] = useState('');
  const [repoPath, setRepoPath] = useState('');
  const [repoSelected, setRepoSelected] = useState(false);
  const [isConfigured, setIsConfigured] = useState(false);

  const [sourceType] = useState<SourceType>('sql_server');
  const [sourceServer, setSourceServer] = useState('');
  const [sourceDatabase, setSourceDatabase] = useState('');
  const [sourceDatabases, setSourceDatabases] = useState<string[]>([]);
  const [sourcePort, setSourcePort] = useState(String(SOURCE_DEFAULTS.port));
  const [sourceAuthenticationMode, setSourceAuthenticationMode] =
    useState<SourceAuthenticationMode>(SOURCE_DEFAULTS.authenticationMode);
  const [sourceUsername, setSourceUsername] = useState('');
  const [sourcePassword, setSourcePassword] = useState('');
  const [sourceEncrypt, setSourceEncrypt] = useState(SOURCE_DEFAULTS.encrypt);
  const [sourceTrustServerCertificate, setSourceTrustServerCertificate] = useState(
    SOURCE_DEFAULTS.trustServerCertificate,
  );

  const [errors, setErrors] = useState<{
    repoName?: string;
    repoPath?: string;
    sourceServer?: string;
    sourceDatabase?: string;
    sourcePort?: string;
    sourceUsername?: string;
    sourcePassword?: string;
  }>({});
  const [applying, setApplying] = useState(false);
  const [resetDialogOpen, setResetDialogOpen] = useState(false);
  const [resetConfirmationInput, setResetConfirmationInput] = useState('');
  const [testingConnection, setTestingConnection] = useState(false);
  const [testConnectionMessage, setTestConnectionMessage] = useState<string | null>(null);
  const [testConnectionError, setTestConnectionError] = useState<string | null>(null);
  const [connectionTestPassed, setConnectionTestPassed] = useState(false);
  const [applyError, setApplyError] = useState<string | null>(null);
  const [applySuccessMessage, setApplySuccessMessage] = useState<string | null>(null);
  const [applyProgressMessage, setApplyProgressMessage] = useState<string | null>(null);
  const [applyProgressPercent, setApplyProgressPercent] = useState<number>(0);
  const [resetError, setResetError] = useState<string | null>(null);
  const progressTimerRef = useRef<number | null>(null);
  const applyJobIdRef = useRef<string | null>(null);
  const applyPollerRef = useRef<number | null>(null);

  const [repoIndex, setRepoIndex] = useState<GitHubRepo[]>([]);
  const [suggestionsLoading, setSuggestionsLoading] = useState(false);
  const repoIndexLoadedRef = useRef(false);

  useEffect(() => {
    workspaceGet()
      .then((ws) => {
        if (!ws) return;

        setWorkspaceId(ws.id);
        setWorkspaceName(ws.displayName || DEFAULT_WORKSPACE_NAME);
        setRepoName(ws.migrationRepoName ?? '');
        setRepoPath(ws.migrationRepoPath ?? '');
        setRepoSelected(Boolean(ws.migrationRepoName));
        setIsConfigured(Boolean(ws.migrationRepoName && ws.migrationRepoPath));

        setSourceServer(ws.sourceServer ?? '');
        const initialDatabase = ws.sourceDatabase ?? '';
        setSourceDatabase(initialDatabase);
        setSourceDatabases(initialDatabase ? [initialDatabase] : []);
        setSourcePort(String(ws.sourcePort ?? SOURCE_DEFAULTS.port));
        setSourceAuthenticationMode(ws.sourceAuthenticationMode ?? SOURCE_DEFAULTS.authenticationMode);
        setSourceUsername(ws.sourceUsername ?? ws.fabricServicePrincipalId ?? '');
        setSourcePassword(''); // password not returned by backend (security) — user must re-enter to change
        setSourceEncrypt(ws.sourceEncrypt ?? SOURCE_DEFAULTS.encrypt);
        setSourceTrustServerCertificate(
          ws.sourceTrustServerCertificate ?? SOURCE_DEFAULTS.trustServerCertificate,
        );
      })
      .catch((e) => logger.error('workspace_get failed', e));
  }, [setWorkspaceId]);

  const pageLocked = isLocked || isConfigured;

  useEffect(() => {
    if (!applying) {
      if (progressTimerRef.current !== null) {
        window.clearInterval(progressTimerRef.current);
        progressTimerRef.current = null;
      }
      if (applyPollerRef.current !== null) {
        window.clearInterval(applyPollerRef.current);
        applyPollerRef.current = null;
      }
      applyJobIdRef.current = null;
      return;
    }

    if (progressTimerRef.current !== null) {
      window.clearInterval(progressTimerRef.current);
    }
    progressTimerRef.current = window.setInterval(() => {
      // Fallback UI progress in case backend events are delayed/missed.
      setApplyProgressPercent((prev) => (prev >= 92 ? prev : prev + 2));
      setApplyProgressMessage((prev) => prev ?? 'Importing source metadata...');
    }, 700);

    return () => {
      if (progressTimerRef.current !== null) {
        window.clearInterval(progressTimerRef.current);
        progressTimerRef.current = null;
      }
      if (applyPollerRef.current !== null) {
        window.clearInterval(applyPollerRef.current);
        applyPollerRef.current = null;
      }
    };
  }, [applying]);

  const resetToken = `RESET ${sourceDatabase.trim()}`;

  function clearWorkspaceForm() {
    setWorkspaceName(DEFAULT_WORKSPACE_NAME);
    setRepoName('');
    setRepoPath('');
    setRepoSelected(false);

    setSourceServer('');
    setSourceDatabase('');
    setSourceDatabases([]);
    setSourcePort(String(SOURCE_DEFAULTS.port));
    setSourceAuthenticationMode(SOURCE_DEFAULTS.authenticationMode);
    setSourceUsername('');
    setSourcePassword('');
    setSourceEncrypt(SOURCE_DEFAULTS.encrypt);
    setSourceTrustServerCertificate(SOURCE_DEFAULTS.trustServerCertificate);

    setConnectionTestPassed(false);
    setTestConnectionMessage(null);
    setTestConnectionError(null);
    setApplyError(null);
    setApplySuccessMessage(null);
    setResetError(null);
    setErrors({});
  }

  function validate(): boolean {
    const errs: {
      repoName?: string;
      repoPath?: string;
      sourceServer?: string;
      sourceDatabase?: string;
      sourcePort?: string;
      sourceUsername?: string;
      sourcePassword?: string;
    } = {};

    if (!repoName.trim()) errs.repoName = 'Repo selection is required';
    else if (!repoSelected) errs.repoName = 'Select a repo from the picklist';

    if (!repoPath.trim()) errs.repoPath = 'Repo path is required';
    if (!sourceServer.trim()) errs.sourceServer = 'Server is required';
    if (!sourceDatabase.trim()) errs.sourceDatabase = 'Database is required';

    const parsedPort = Number(sourcePort);
    if (!sourcePort.trim()) errs.sourcePort = 'Port is required';
    else if (!Number.isInteger(parsedPort) || parsedPort <= 0) {
      errs.sourcePort = 'Port must be a positive integer';
    }

    if (!sourceUsername.trim()) {
      errs.sourceUsername =
        sourceAuthenticationMode === 'sql_password'
          ? 'SQL login is required'
          : 'Service principal ID is required';
    }
    if (!sourcePassword.trim()) {
      errs.sourcePassword =
        sourceAuthenticationMode === 'sql_password' ? 'Password is required' : 'Secret is required';
    }

    setErrors(errs);
    return Object.keys(errs).length === 0;
  }

  async function pickDirectory() {
    const selected = await openDialog({ directory: true, multiple: false });
    if (typeof selected === 'string') setRepoPath(selected);
  }

  async function ensureRepoIndexLoaded() {
    if (repoIndexLoadedRef.current || suggestionsLoading) return;
    setSuggestionsLoading(true);
    try {
      const repos = await githubListRepos('', 100);
      repoIndexLoadedRef.current = true;
      setRepoIndex(repos);
    } catch (err) {
      logger.error('workspace repo list preload failed', err);
      setRepoIndex([]);
    } finally {
      setSuggestionsLoading(false);
    }
  }

  function invalidateConnectionTestState() {
    if (connectionTestPassed) setConnectionTestPassed(false);
    if (testConnectionMessage) setTestConnectionMessage(null);
    if (testConnectionError) setTestConnectionError(null);
    if (applySuccessMessage) setApplySuccessMessage(null);
    if (sourceDatabases.length > 0) setSourceDatabases([]);
    if (sourceDatabase) setSourceDatabase('');
  }

  async function handleApply() {
    if (!validate()) return;

    const sourceServerValue = sourceServer.trim();
    const sourceDatabaseValue = sourceDatabase.trim();
    const sourcePortValue = Number(sourcePort);
    const sourceUsernameValue = sourceUsername.trim();
    const sourcePasswordValue = sourcePassword.trim();

    setApplying(true);
    setApplyError(null);
    setApplySuccessMessage(null);
    setApplyProgressMessage('Validating source access...');
    setApplyProgressPercent(8);

    // Ensure the progress UI paints before the apply job starts.
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => resolve());
    });

    try {
      const jobId = await workspaceApplyStart({
        name: workspaceName.trim() || DEFAULT_WORKSPACE_NAME,
        migrationRepoName: repoName.trim(),
        migrationRepoPath: repoPath.trim(),
        fabricUrl: null,
        fabricServicePrincipalId: null,
        fabricServicePrincipalSecret: null,
        sourceType,
        sourceServer: sourceServerValue,
        sourceDatabase: sourceDatabaseValue,
        sourcePort: sourcePortValue,
        sourceAuthenticationMode,
        sourceUsername: sourceUsernameValue,
        sourcePassword: sourcePasswordValue,
        sourceEncrypt,
        sourceTrustServerCertificate,
      });
      applyJobIdRef.current = jobId;

      const pollStatus = async () => {
        if (!applyJobIdRef.current) return;
        const status = await workspaceApplyStatus(applyJobIdRef.current);
        if (status.message) setApplyProgressMessage(status.message);
        setApplyProgressPercent(status.percent);

        if (status.state === 'running') return;

        if (applyPollerRef.current !== null) {
          window.clearInterval(applyPollerRef.current);
          applyPollerRef.current = null;
        }

        if (status.state === 'succeeded') {
          const ws = await workspaceGet();
          if (ws) {
            setWorkspaceId(ws.id);
            setWorkspaceName(ws.displayName);
          }
          try {
            const next = await appHydratePhase();
            setAppPhaseState(next);
          } catch (err) {
            logger.error('workspace: failed to hydrate app phase after apply', err);
          }
          setIsConfigured(true);
          setApplySuccessMessage('Workspace applied successfully. Repository cloned locally.');
          setApplyProgressMessage(null);
          setApplyProgressPercent(100);
          logger.info('workspace: applied and repo cloned');
          setApplying(false);
          return;
        }

        logger.error('workspace apply failed');
        setApplyError(status.error ?? 'Apply failed');
        setApplyProgressMessage(null);
        setApplyProgressPercent(0);
        setApplying(false);
      };

      await pollStatus();
      applyPollerRef.current = window.setInterval(() => {
        void pollStatus();
      }, 600);
    } catch (err) {
      const message = getErrorMessage(err);
      logger.error('workspace apply failed', err);
      setApplyError(message);
      setApplyProgressMessage(null);
      setApplyProgressPercent(0);
      setApplying(false);
    }
  }

  async function handleResetMigration() {
    setResetError(null);
    try {
      await workspaceResetState();
      reset();
      clearWorkspaceId();
      setIsConfigured(false);
      setResetConfirmationInput('');
      setResetDialogOpen(false);
      clearWorkspaceForm();
      try {
        const next = await appHydratePhase();
        setAppPhaseState(next);
      } catch (hydrateErr) {
        logger.error('workspace reset: failed to hydrate app phase', hydrateErr);
      }
      logger.info('workspace: reset migration state');
    } catch (err) {
      const message = getErrorMessage(err);
      setResetError(message);
      logger.error('workspace reset failed', err);
    }
  }

  async function handleTestConnection() {
    const sourceServerValue = sourceServer.trim();
    const sourcePortValue = Number(sourcePort);
    const sourceUsernameValue = sourceUsername.trim();
    const sourcePasswordValue = sourcePassword.trim();
    if (
      !sourceServerValue ||
      !sourceUsernameValue ||
      !sourcePasswordValue ||
      !Number.isInteger(sourcePortValue) ||
      sourcePortValue <= 0
    ) {
      setTestConnectionMessage(null);
      setTestConnectionError(
        'Enter server, credentials, and a valid port before testing connection.',
      );
      return;
    }

    setTestingConnection(true);
    setTestConnectionMessage(null);
    setTestConnectionError(null);
    try {
      const message = await workspaceTestSourceConnection({
        sourceType,
        sourceServer: sourceServerValue,
        sourcePort: sourcePortValue,
        sourceAuthenticationMode,
        sourceUsername: sourceUsernameValue,
        sourcePassword: sourcePasswordValue,
        sourceEncrypt,
        sourceTrustServerCertificate,
      });
      const databases = await workspaceDiscoverSourceDatabases({
        sourceType,
        sourceServer: sourceServerValue,
        sourcePort: sourcePortValue,
        sourceAuthenticationMode,
        sourceUsername: sourceUsernameValue,
        sourcePassword: sourcePasswordValue,
        sourceEncrypt,
        sourceTrustServerCertificate,
      });
      setSourceDatabases(databases);
      if (databases.length === 0) {
        setSourceDatabase('');
        setConnectionTestPassed(false);
        setTestConnectionMessage(null);
        setTestConnectionError('Connection succeeded, but no accessible databases were discovered.');
        return;
      }
      setSourceDatabase((current) => (databases.includes(current) ? current : databases[0]));
      setTestConnectionMessage(message);
      setConnectionTestPassed(true);
      logger.info('workspace: source connection test passed');
    } catch (err) {
      const message = getErrorMessage(err);
      setTestConnectionError(message);
      setConnectionTestPassed(false);
      logger.error('workspace source connection test failed', err);
    } finally {
      setTestingConnection(false);
    }
  }

  const isSqlAuth = sourceAuthenticationMode === 'sql_password';

  const canApply = !pageLocked && !applying && connectionTestPassed && repoSelected && !!repoPath.trim();

  return (
    <SettingsPanelShell
      panelTestId="settings-panel-workspace"
      labelTestId="settings-workspace-group-label"
      groupLabel={
        <span className="inline-flex items-center gap-1.5">
          {pageLocked ? <Lock className="size-3" /> : null}
          {pageLocked ? 'Workspace configured and locked' : 'Set once per migration'}
        </span>
      }
    >
      <div data-testid="settings-workspace-tab" className="flex flex-col gap-3">
        {pageLocked ? (
          <p className="text-sm text-amber-700 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-md px-3 py-2">
            Workspace is locked after apply. Reset Migration to edit settings.
          </p>
        ) : null}

        <Card className="gap-0 py-5" data-testid="settings-workspace-fabric-card">
          <CardHeader className="pb-3">
            <CardTitle>Source connection</CardTitle>
            <CardDescription className="mt-0.5">
              Configure source credentials, test connectivity, then pick a discovered database.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex flex-col gap-3">
            <div className="flex flex-col gap-2">
              <div className="flex flex-col gap-1">
                <Label htmlFor="source-type">Data source type</Label>
                <select
                  id="source-type"
                  data-testid="select-source-type"
                  value={sourceType}
                  onChange={() => {}}
                  className="h-9 rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs outline-none focus-visible:ring-2 focus-visible:ring-ring/50"
                  disabled
                >
                  <option value="sql_server">SQL Server</option>
                </select>
              </div>
            </div>

            <div className="flex flex-col gap-2">
              <div className="flex flex-col gap-1">
                <Label htmlFor="source-server">Server</Label>
                <Input
                  id="source-server"
                  data-testid="input-source-server"
                  type="text"
                  value={sourceServer}
                  onChange={(e) => {
                    setSourceServer(e.target.value);
                    invalidateConnectionTestState();
                  }}
                  placeholder="sqlserver.example.com"
                  className="font-mono text-sm"
                  disabled={pageLocked}
                />
                {errors.sourceServer ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.sourceServer}
                  </p>
                ) : null}
              </div>
              <div className="flex flex-col gap-1">
                <Label htmlFor="source-port">Port</Label>
                <Input
                  id="source-port"
                  data-testid="input-source-port"
                  type="text"
                  value={sourcePort}
                  onChange={(e) => {
                    setSourcePort(e.target.value);
                    invalidateConnectionTestState();
                  }}
                  placeholder="1433"
                  className="font-mono text-sm"
                  disabled={pageLocked}
                />
                {errors.sourcePort ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.sourcePort}
                  </p>
                ) : null}
              </div>
            </div>

            <div className="flex flex-col gap-1">
              <Label htmlFor="source-auth-mode">Authentication</Label>
              <select
                id="source-auth-mode"
                data-testid="select-source-authentication-mode"
                value={sourceAuthenticationMode}
                onChange={(e) => {
                  setSourceAuthenticationMode(e.target.value as SourceAuthenticationMode);
                  invalidateConnectionTestState();
                }}
                className="h-9 rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs outline-none focus-visible:ring-2 focus-visible:ring-ring/50"
                disabled={pageLocked}
              >
                <option value="sql_password">SQL Login</option>
                <option value="entra_service_principal">Entra Service Principal</option>
              </select>
            </div>

            <div className="flex flex-col gap-2">
              <div className="flex flex-col gap-1">
                <Label htmlFor="source-username" className="text-sm text-muted-foreground">
                  {isSqlAuth ? 'SQL login' : 'Service principal ID'}
                </Label>
                <Input
                  id="source-username"
                  data-testid="input-source-username"
                  type="text"
                  value={sourceUsername}
                  onChange={(e) => {
                    setSourceUsername(e.target.value);
                    invalidateConnectionTestState();
                  }}
                  placeholder={isSqlAuth ? 'sa' : 'sp-vibedata-migration'}
                  className="font-mono text-sm"
                  disabled={pageLocked}
                />
                {errors.sourceUsername ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.sourceUsername}
                  </p>
                ) : null}
              </div>
              <div className="flex flex-col gap-1">
                <Label htmlFor="source-password" className="text-sm text-muted-foreground">
                  {isSqlAuth ? 'Password' : 'Secret'}
                </Label>
                <Input
                  id="source-password"
                  data-testid="input-source-password"
                  type="password"
                  value={sourcePassword}
                  onChange={(e) => {
                    setSourcePassword(e.target.value);
                    invalidateConnectionTestState();
                  }}
                  placeholder="••••••••••••"
                  className="font-mono text-sm"
                  disabled={pageLocked}
                />
                {errors.sourcePassword ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.sourcePassword}
                  </p>
                ) : null}
              </div>
            </div>

            <div className="flex flex-col gap-2">
              <label className="inline-flex items-center gap-2 text-sm" htmlFor="source-encrypt">
                <input
                  id="source-encrypt"
                  data-testid="checkbox-source-encrypt"
                  type="checkbox"
                  checked={sourceEncrypt}
                  onChange={(e) => {
                    setSourceEncrypt(e.target.checked);
                    invalidateConnectionTestState();
                  }}
                  disabled={pageLocked}
                />
                Encrypt connection
              </label>
              <label
                className="inline-flex items-center gap-2 text-sm"
                htmlFor="source-trust-server-certificate"
              >
                <input
                  id="source-trust-server-certificate"
                  data-testid="checkbox-source-trust-server-certificate"
                  type="checkbox"
                  checked={sourceTrustServerCertificate}
                  onChange={(e) => {
                    setSourceTrustServerCertificate(e.target.checked);
                    invalidateConnectionTestState();
                  }}
                  disabled={pageLocked}
                />
                Trust server certificate
              </label>
            </div>

            <div className="rounded-md border border-border bg-muted/40 px-3 py-3 flex flex-col gap-2">
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm text-muted-foreground">
                  Step 1: test server/auth credentials
                </div>
                <Button
                  type="button"
                  data-testid="btn-test-connection"
                  onClick={handleTestConnection}
                  disabled={pageLocked || testingConnection}
                  variant="outline"
                  size="sm"
                >
                  {testingConnection ? 'Testing…' : 'Test connection'}
                </Button>
              </div>
              <div className="text-xs text-muted-foreground">
                Step 2: select database from discovered list
              </div>
              <div className="flex flex-col gap-1">
                <Label htmlFor="source-database">Database</Label>
                <select
                  id="source-database"
                  data-testid="input-source-database"
                  value={sourceDatabase}
                  onChange={(e) => {
                    setSourceDatabase(e.target.value);
                  }}
                  className="h-9 rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs outline-none focus-visible:ring-2 focus-visible:ring-ring/50 font-mono"
                  disabled={pageLocked || sourceDatabases.length === 0}
                >
                  {sourceDatabases.length === 0 ? (
                    <option value="">Test connection to discover databases</option>
                  ) : null}
                  {sourceDatabases.map((db) => (
                    <option key={db} value={db}>
                      {db}
                    </option>
                  ))}
                </select>
                {errors.sourceDatabase ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.sourceDatabase}
                  </p>
                ) : null}
              </div>
            </div>

            {testConnectionError ? (
              <p
                className="text-xs text-destructive"
                role="alert"
                data-testid="workspace-test-connection-error"
              >
                {testConnectionError}
              </p>
            ) : null}
            {testConnectionMessage ? (
              <p
                className="text-xs"
                style={{ color: 'var(--color-seafoam)' }}
                data-testid="workspace-test-connection-success"
              >
                {testConnectionMessage}
              </p>
            ) : null}
          </CardContent>
        </Card>

        <Card className="gap-0 py-5" data-testid="settings-workspace-repo-card">
          <CardHeader className="pb-3">
            <CardTitle>Migration repo</CardTitle>
            <CardDescription className="mt-0.5">
              Select from repositories you can access.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex flex-col gap-3">
            <div className="flex flex-col gap-1 relative">
              <Label htmlFor="repo-name">
                Repo name <span className="text-destructive">*</span>
              </Label>
              <select
                id="repo-name"
                data-testid="input-repo-name"
                value={repoName}
                onFocus={() => {
                  void ensureRepoIndexLoaded();
                }}
                onChange={(e) => {
                  const value = e.target.value;
                  setRepoName(value);
                  setRepoSelected(Boolean(value));
                }}
                className="h-9 rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs outline-none focus-visible:ring-2 focus-visible:ring-ring/50 font-mono"
                disabled={pageLocked}
              >
                <option value="">
                  {suggestionsLoading ? 'Loading repositories…' : 'Select repository'}
                </option>
                {repoName && !repoIndex.some((repo) => repo.fullName === repoName) ? (
                  <option value={repoName}>{repoName}</option>
                ) : null}
                {repoIndex.map((repo) => (
                  <option key={repo.id} value={repo.fullName}>
                    {repo.fullName}
                  </option>
                ))}
              </select>

              {errors.repoName ? (
                <p className="text-xs text-destructive" role="alert">
                  {errors.repoName}
                </p>
              ) : null}
            </div>
          </CardContent>
        </Card>

        <Card className="gap-0 py-5" data-testid="settings-workspace-working-dir-card">
          <CardHeader className="pb-3">
            <CardTitle>Working directory</CardTitle>
            <CardDescription className="mt-0.5">
              Choose where the selected repository should be cloned locally.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex flex-col gap-3">
            <div className="flex flex-col gap-1">
              <Label htmlFor="repo-path">
                Directory <span className="text-destructive">*</span>
              </Label>
              <div className="flex gap-2">
                <Input
                  id="repo-path"
                  data-testid="input-repo-path"
                  type="text"
                  value={repoPath}
                  onChange={() => {}}
                  className="flex-1"
                  placeholder="/path/to/local-clone"
                  disabled
                  readOnly
                />
                <Button
                  type="button"
                  data-testid="btn-pick-repo-path"
                  variant="outline"
                  onClick={pickDirectory}
                  disabled={pageLocked}
                >
                  Browse
                </Button>
              </div>
              {errors.repoPath ? (
                <p className="text-xs text-destructive" role="alert">
                  {errors.repoPath}
                </p>
              ) : null}
            </div>
          </CardContent>
        </Card>

        {applyError ? (
          <p className="text-xs text-destructive" role="alert" data-testid="workspace-apply-error">
            {applyError}
          </p>
        ) : null}
        {applySuccessMessage ? (
          <p
            className="text-xs"
            style={{ color: 'var(--color-seafoam)' }}
            data-testid="workspace-apply-success"
          >
            {applySuccessMessage}
          </p>
        ) : null}
        {applying && applyProgressMessage ? (
          <div className="flex flex-col gap-1" data-testid="workspace-apply-progress">
            <p className="text-xs text-muted-foreground">{applyProgressMessage}</p>
            <div className="h-1.5 rounded-full bg-muted overflow-hidden">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{ width: `${applyProgressPercent}%` }}
                data-testid="workspace-apply-progress-bar"
              />
            </div>
          </div>
        ) : null}
        {resetError ? (
          <p className="text-xs text-destructive" role="alert" data-testid="workspace-reset-error">
            {resetError}
          </p>
        ) : null}

        <div className="flex items-center justify-end gap-2" data-testid="settings-workspace-actions">
          <Button
            type="button"
            data-testid="btn-apply"
            onClick={handleApply}
            disabled={!canApply}
            size="sm"
          >
            {applying ? 'Applying…' : 'Apply'}
          </Button>
        </div>

        <Card className="gap-0 py-5 border-destructive/40" data-testid="settings-workspace-danger-zone">
          <CardHeader className="pb-3">
            <CardTitle className="text-destructive">Danger Zone</CardTitle>
            <CardDescription className="mt-0.5">
              Reset local migration state and workspace settings. Remote GitHub repository is not touched.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex flex-col gap-3">
            <Button
              type="button"
              data-testid="btn-open-reset-migration-dialog"
              variant="destructive"
              size="sm"
              onClick={() => setResetDialogOpen(true)}
            >
              Reset Migration
            </Button>
          </CardContent>
        </Card>
      </div>

      <Dialog open={resetDialogOpen} onOpenChange={setResetDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Reset migration</DialogTitle>
            <DialogDescription>
              This clears local migration state and workspace settings. Remote GitHub repository is not touched.
            </DialogDescription>
          </DialogHeader>

          <div className="flex flex-col gap-2">
            <p className="text-sm text-muted-foreground">
              Type <code>{resetToken}</code> to confirm.
            </p>
            <Input
              data-testid="input-reset-confirmation"
              type="text"
              value={resetConfirmationInput}
              onChange={(e) => setResetConfirmationInput(e.target.value)}
              placeholder={resetToken}
              className="font-mono text-sm"
            />
          </div>

          <div className="flex justify-end gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => {
                setResetDialogOpen(false);
                setResetConfirmationInput('');
              }}
            >
              Cancel
            </Button>
            <Button
              type="button"
              variant="destructive"
              data-testid="btn-confirm-reset-migration"
              onClick={() => void handleResetMigration()}
              disabled={resetConfirmationInput.trim() !== resetToken}
            >
              Reset Migration
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </SettingsPanelShell>
  );
}
