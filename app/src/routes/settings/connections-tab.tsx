import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, ChevronsUpDown, FolderOpen, Github, Loader2, LogOut, XCircle } from 'lucide-react';
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
import { githubCheckRepoEmpty, githubListRepos, saveRepoSettings } from '@/lib/tauri';
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

  // ── Repo selector state ──────────────────────────────────────────────────────
  const [repoQuery, setRepoQuery] = useState('');
  const [repos, setRepos] = useState<GitHubRepo[]>([]);
  const [repoDropOpen, setRepoDropOpen] = useState(false);
  const [selectedRepo, setSelectedRepo] = useState<GitHubRepo | null>(null);
  const [repoEmptyStatus, setRepoEmptyStatus] = useState<'idle' | 'checking' | 'empty' | 'not-empty'>('idle');
  const [reposLoading, setReposLoading] = useState(false);
  const searchDebounce = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // ── Local path state ─────────────────────────────────────────────────────────
  const [localPath, setLocalPath] = useState(''); // stored as tilde-compressed display value
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
      setRepoQuery(migrationRepoFullName);
      setRepoEmptyStatus('empty'); // assume previously validated
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

  // Pre-fetch repo list as soon as the user is logged in
  useEffect(() => {
    if (!isLoggedIn || isAuthLoading) return;
    setReposLoading(true);
    githubListRepos('', 100)
      .then(setRepos)
      .catch(() => setRepos([]))
      .finally(() => setReposLoading(false));
  }, [isLoggedIn, isAuthLoading]);

  // Dismiss dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setRepoDropOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  function handleRepoQueryChange(value: string) {
    setRepoQuery(value);
    setSelectedRepo(null);
    setRepoEmptyStatus('idle');
    setRepoDropOpen(true);

    if (searchDebounce.current) clearTimeout(searchDebounce.current);
    searchDebounce.current = setTimeout(() => {
      setReposLoading(true);
      githubListRepos(value, 30)
        .then(setRepos)
        .catch(() => setRepos([]))
        .finally(() => setReposLoading(false));
    }, 300);
  }

  function handleRepoFocus() {
    setRepoDropOpen(true);
  }

  function handleSelectRepo(repo: GitHubRepo) {
    setSelectedRepo(repo);
    setRepoQuery(repo.fullName);
    setRepoDropOpen(false);
    setRepoEmptyStatus('checking');
    githubCheckRepoEmpty(repo.fullName)
      .then((empty) => setRepoEmptyStatus(empty ? 'empty' : 'not-empty'))
      .catch(() => setRepoEmptyStatus('idle'));
  }

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

            {/* Repo selector */}
            <div className="flex flex-col gap-1.5" ref={dropdownRef}>
              <Label className="text-xs font-medium text-muted-foreground">Remote repository</Label>
              <div className="relative">
                <div className="flex items-center gap-2">
                  <div className="relative flex-1">
                    <Input
                      data-testid="input-repo-search"
                      value={repoQuery}
                      onChange={(e) => handleRepoQueryChange(e.target.value)}
                      onFocus={handleRepoFocus}
                      placeholder="Search repositories…"
                      className="pr-8 font-mono text-sm"
                      autoComplete="off"
                    />
                    <div className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none">
                      {reposLoading
                        ? <Loader2 className="size-3.5 animate-spin" />
                        : <ChevronsUpDown className="size-3.5" />}
                    </div>
                  </div>
                  {/* Empty check indicator */}
                  {repoEmptyStatus === 'checking' && (
                    <Loader2 className="size-4 shrink-0 animate-spin text-muted-foreground" />
                  )}
                  {repoEmptyStatus === 'empty' && (
                    <CheckCircle2
                      className="size-4 shrink-0"
                      style={{ color: 'var(--color-seafoam)' }}
                      aria-label="Repository is empty"
                    />
                  )}
                  {repoEmptyStatus === 'not-empty' && (
                    <XCircle className="size-4 shrink-0 text-destructive" aria-label="Repository has content" />
                  )}
                </div>

                {/* Dropdown */}
                {repoDropOpen && repos.length > 0 && (
                  <div className="absolute z-50 mt-1 w-full rounded-md border border-border bg-popover shadow-md overflow-hidden">
                    <ul className="max-h-52 overflow-auto py-1" role="listbox">
                      {repos.map((repo) => (
                        <li
                          key={repo.id}
                          role="option"
                          aria-selected={selectedRepo?.id === repo.id}
                          className="flex items-center gap-2 px-3 py-2 text-sm cursor-pointer hover:bg-muted transition-colors duration-100"
                          onMouseDown={(e) => { e.preventDefault(); handleSelectRepo(repo); }}
                        >
                          <span className="font-mono flex-1 truncate">{repo.fullName}</span>
                          {repo.private && (
                            <Badge variant="outline" className="text-[10px] py-0 h-4 shrink-0">private</Badge>
                          )}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
              {repoEmptyStatus === 'not-empty' && (
                <p className="text-xs text-destructive mt-0.5">
                  This repository already has a project folder. Choose a repo without existing directories.
                </p>
              )}
            </div>

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
