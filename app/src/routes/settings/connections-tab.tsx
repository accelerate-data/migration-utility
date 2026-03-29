import { useEffect, useState } from 'react';
import { FolderOpen, Github, Loader2, LogOut } from 'lucide-react';
import { toast } from 'sonner';
import { useAuthStore } from '@/stores/auth-store';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { GitHubLoginDialog } from '@/components/github-login-dialog';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import RepoSelector from '@/components/settings/repo-selector';
import type { RepoEmptyStatus } from '@/components/settings/repo-selector';
import { saveRepoSettings } from '@/lib/tauri';
import { useSettingsStore } from '@/stores/settings-store';
import { logger } from '@/lib/logger';
import type { GitHubRepo } from '@/lib/types';
import { open as openDialog } from '@tauri-apps/plugin-dialog';
import { homeDir } from '@tauri-apps/api/path';
import { prettyPath, expandPath } from '@/lib/path-utils';

export default function ConnectionsTab() {
  const { user, isLoggedIn, isLoading: isAuthLoading, lastCheckedAt, loadUser, logout } = useAuthStore();
  const { migrationRepoFullName, migrationRepoCloneUrl, localClonePath: storedClonePath, loadSettings } = useSettingsStore();
  const [loginDialogOpen, setLoginDialogOpen] = useState(false);

  // ── Repo selector state (lifted for save logic) ────────────────────────────
  const [selectedRepo, setSelectedRepo] = useState<GitHubRepo | null>(null);
  const [repoEmptyStatus, setRepoEmptyStatus] = useState<RepoEmptyStatus>('idle');

  // ── Local path state ─────────────────────────────────────────────────────────
  const [localPath, setLocalPath] = useState('');
  const [homeDirPath, setHomeDirPath] = useState('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadUser();
    void loadSettings();
  }, [loadUser, loadSettings]);

  // Hydrate local UI state from settings store once loaded
  useEffect(() => {
    if (migrationRepoFullName && migrationRepoCloneUrl) {
      setSelectedRepo({
        id: 0,
        fullName: migrationRepoFullName,
        cloneUrl: migrationRepoCloneUrl,
        private: false,
      });
      setRepoEmptyStatus('empty');
    }
    homeDir().then((h) => {
      setHomeDirPath(h);
      if (storedClonePath) {
        const parent = storedClonePath.replace(/\/[^/]+\/?$/, '') || storedClonePath;
        setLocalPath(prettyPath(parent, h));
      } else {
        setLocalPath('~');
      }
    }).catch(() => {
      if (storedClonePath) {
        const parent = storedClonePath.replace(/\/[^/]+\/?$/, '') || storedClonePath;
        setLocalPath(parent);
      }
    });
  }, [migrationRepoFullName, migrationRepoCloneUrl, storedClonePath]);

  async function handleBrowseLocalPath() {
    const selected = await openDialog({ directory: true, multiple: false, title: 'Select parent folder for migration repo' });
    if (typeof selected === 'string') setLocalPath(prettyPath(selected, homeDirPath));
  }

  async function handleSave() {
    if (!selectedRepo || !localPath.trim()) return;
    setSaving(true);
    try {
      await saveRepoSettings(selectedRepo.fullName, selectedRepo.cloneUrl, expandPath(localPath.trim(), homeDirPath));
      await loadSettings();
      toast.success('Repository cloned and settings saved');
      logger.info('settings: repo settings saved repo=%s', selectedRepo.fullName);
    } catch (err) {
      logger.error('save_repo_settings failed', err);
      const msg = err && typeof err === 'object' && 'message' in err ? (err as { message: string }).message : String(err);
      toast.error(`Failed to save repository settings: ${msg}`, { duration: Infinity });
    } finally {
      setSaving(false);
    }
  }

  const clonePreview = storedClonePath ?? null;
  const githubStatus = isAuthLoading ? 'Checking' : isLoggedIn && user ? 'Connected' : 'Not connected';
  const canSave = selectedRepo !== null && repoEmptyStatus === 'empty' && localPath.trim().length > 0;

  return (
    <SettingsPanelShell panelTestId="settings-panel-connections">

      {/* GitHub auth card */}
      <Card className="gap-0 py-5" data-testid="settings-connections-github-card">
        <CardHeader className="pb-3">
          <div className="flex items-center gap-2">
            <CardTitle>GitHub</CardTitle>
            <Badge className="text-sm" variant={isLoggedIn && !isAuthLoading ? 'secondary' : 'outline'}>
              {githubStatus}
            </Badge>
          </div>
          <CardDescription className="mt-0.5">
            Used to clone and push to your migration repo.
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-0">
          {isAuthLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              Checking GitHub connection...
            </div>
          ) : isLoggedIn && user ? (
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-start gap-2.5 min-w-0">
                <Avatar className="size-8 shrink-0">
                  <AvatarImage src={user.avatar_url} alt={user.login} />
                  <AvatarFallback>{user.login.slice(0, 2).toUpperCase()}</AvatarFallback>
                </Avatar>
                <div className="min-w-0">
                  <p className="text-sm font-medium leading-tight">@{user.login}</p>
                  {user.email ? (
                    <p className="text-sm text-muted-foreground leading-tight mt-0.5">{user.email}</p>
                  ) : null}
                  {lastCheckedAt ? (
                    <p className="text-sm text-muted-foreground mt-1">
                      Last checked {new Date(lastCheckedAt).toLocaleString()}
                    </p>
                  ) : null}
                </div>
              </div>
              <Button variant="outline" data-testid="btn-disconnect-github" onClick={logout}>
                <LogOut className="size-3.5" />
                Sign Out
              </Button>
            </div>
          ) : (
            <div className="flex items-center justify-between">
              <p className="text-sm text-muted-foreground">Not connected</p>
              <Button variant="outline" data-testid="btn-connect-github" onClick={() => setLoginDialogOpen(true)}>
                <Github className="size-3.5" />
                Sign in with GitHub
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Migration repo card — only shown when GitHub is connected */}
      {isLoggedIn && !isAuthLoading ? (
        <Card className="gap-0 py-5" data-testid="settings-connections-repo-card">
          <CardHeader className="pb-3">
            <CardTitle>Migration Repository</CardTitle>
            <CardDescription className="mt-0.5">
              Must be an empty GitHub repo. All migration artifacts will be committed here.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex flex-col gap-4">

            <RepoSelector
              selectedRepo={selectedRepo}
              onSelectRepo={setSelectedRepo}
              onClear={() => setSelectedRepo(null)}
              emptyStatus={repoEmptyStatus}
              onEmptyStatusChange={setRepoEmptyStatus}
              initialQuery={migrationRepoFullName ?? ''}
              isLoggedIn={isLoggedIn}
              isAuthLoading={isAuthLoading}
            />

            {/* Local parent folder */}
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs font-medium text-muted-foreground">Parent folder</Label>
              <div className="flex gap-2">
                <Input
                  data-testid="input-local-clone-path"
                  value={localPath}
                  onChange={(e) => setLocalPath(e.target.value)}
                  placeholder="~/src"
                  className="font-mono text-sm flex-1"
                />
                <Button
                  type="button"
                  variant="outline"
                  size="icon"
                  data-testid="btn-browse-local-path"
                  onClick={() => { void handleBrowseLocalPath(); }}
                  aria-label="Browse for directory"
                >
                  <FolderOpen className="size-3.5" />
                </Button>
              </div>
              {clonePreview ? (
                <p className="text-xs text-muted-foreground font-mono">
                  Will clone to: {clonePreview}
                </p>
              ) : (
                <p className="text-xs text-muted-foreground">
                  The migration repo will be cloned into a subfolder here.
                </p>
              )}
            </div>

            {/* Save */}
            <div className="flex justify-end pt-1">
              <Button
                data-testid="btn-save-repo-settings"
                onClick={() => { void handleSave(); }}
                disabled={!canSave || saving}
              >
                {saving && <Loader2 className="size-3.5 animate-spin" />}
                {saving ? 'Applying…' : 'Apply'}
              </Button>
            </div>

          </CardContent>
        </Card>
      ) : null}

      <GitHubLoginDialog open={loginDialogOpen} onOpenChange={setLoginDialogOpen} />
    </SettingsPanelShell>
  );
}
