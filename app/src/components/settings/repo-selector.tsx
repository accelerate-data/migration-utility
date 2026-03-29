import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, ChevronsUpDown, Loader2, XCircle } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { githubCheckRepoEmpty, githubListRepos } from '@/lib/tauri';
import type { GitHubRepo } from '@/lib/types';

export type RepoEmptyStatus = 'idle' | 'checking' | 'empty' | 'not-empty';

interface RepoSelectorProps {
  /** Currently selected repo (controlled). */
  selectedRepo: GitHubRepo | null;
  onSelectRepo: (repo: GitHubRepo) => void;
  /** Fires when the user clears the selection by typing. */
  onClear: () => void;
  emptyStatus: RepoEmptyStatus;
  onEmptyStatusChange: (status: RepoEmptyStatus) => void;
  /** Initial query text (e.g. from persisted settings). */
  initialQuery?: string;
  isLoggedIn: boolean;
  isAuthLoading: boolean;
}

export default function RepoSelector({
  selectedRepo,
  onSelectRepo,
  onClear,
  emptyStatus,
  onEmptyStatusChange,
  initialQuery = '',
  isLoggedIn,
  isAuthLoading,
}: RepoSelectorProps) {
  const [query, setQuery] = useState(initialQuery);
  const [repos, setRepos] = useState<GitHubRepo[]>([]);
  const [dropOpen, setDropOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const searchDebounce = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Sync controlled initial query when it changes externally
  useEffect(() => {
    if (initialQuery) setQuery(initialQuery);
  }, [initialQuery]);

  // Pre-fetch repo list when logged in
  useEffect(() => {
    if (!isLoggedIn || isAuthLoading) return;
    setLoading(true);
    githubListRepos('', 100)
      .then(setRepos)
      .catch(() => setRepos([]))
      .finally(() => setLoading(false));
  }, [isLoggedIn, isAuthLoading]);

  // Dismiss dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDropOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  function handleQueryChange(value: string) {
    setQuery(value);
    onClear();
    onEmptyStatusChange('idle');
    setDropOpen(true);

    if (searchDebounce.current) clearTimeout(searchDebounce.current);
    searchDebounce.current = setTimeout(() => {
      setLoading(true);
      githubListRepos(value, 30)
        .then(setRepos)
        .catch(() => setRepos([]))
        .finally(() => setLoading(false));
    }, 300);
  }

  function handleSelect(repo: GitHubRepo) {
    onSelectRepo(repo);
    setQuery(repo.fullName);
    setDropOpen(false);
    onEmptyStatusChange('checking');
    githubCheckRepoEmpty(repo.fullName)
      .then((empty) => onEmptyStatusChange(empty ? 'empty' : 'not-empty'))
      .catch(() => onEmptyStatusChange('idle'));
  }

  return (
    <div className="flex flex-col gap-1.5" ref={dropdownRef}>
      <Label className="text-xs font-medium text-muted-foreground">Remote repository</Label>
      <div className="relative">
        <div className="flex items-center gap-2">
          <div className="relative flex-1">
            <Input
              data-testid="input-repo-search"
              value={query}
              onChange={(e) => handleQueryChange(e.target.value)}
              onFocus={() => setDropOpen(true)}
              placeholder="Search repositories…"
              className="pr-8 font-mono text-sm"
              autoComplete="off"
            />
            <div className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none">
              {loading
                ? <Loader2 className="size-3.5 animate-spin" />
                : <ChevronsUpDown className="size-3.5" />}
            </div>
          </div>
          {emptyStatus === 'checking' && (
            <Loader2 className="size-4 shrink-0 animate-spin text-muted-foreground" />
          )}
          {emptyStatus === 'empty' && (
            <CheckCircle2
              className="size-4 shrink-0"
              style={{ color: 'var(--color-seafoam)' }}
              aria-label="Repository is empty"
            />
          )}
          {emptyStatus === 'not-empty' && (
            <XCircle className="size-4 shrink-0 text-destructive" aria-label="Repository has content" />
          )}
        </div>

        {dropOpen && repos.length > 0 && (
          <div className="absolute z-50 mt-1 w-full rounded-md border border-border bg-popover shadow-md overflow-hidden">
            <ul className="max-h-52 overflow-auto py-1" role="listbox">
              {repos.map((repo) => (
                <li
                  key={repo.id}
                  role="option"
                  aria-selected={selectedRepo?.id === repo.id}
                  className="flex items-center gap-2 px-3 py-2 text-sm cursor-pointer hover:bg-muted transition-colors duration-100"
                  onMouseDown={(e) => { e.preventDefault(); handleSelect(repo); }}
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
      {emptyStatus === 'not-empty' && (
        <p className="text-xs text-destructive mt-0.5">
          This repository already has a project folder. Choose a repo without existing directories.
        </p>
      )}
    </div>
  );
}
