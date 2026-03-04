import { useEffect, useState } from 'react';
import { Github, Loader2, LogOut } from 'lucide-react';
import { useAuthStore } from '@/stores/auth-store';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { GitHubLoginDialog } from '@/components/github-login-dialog';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';

export default function ConnectionsTab() {
  const { user, isLoggedIn, isLoading: isAuthLoading, lastCheckedAt, loadUser, logout } = useAuthStore();
  const [loginDialogOpen, setLoginDialogOpen] = useState(false);

  useEffect(() => {
    loadUser();
  }, [loadUser]);

  const githubStatus = isAuthLoading ? 'Checking' : isLoggedIn && user ? 'Connected' : 'Not connected';

  return (
    <SettingsPanelShell panelTestId="settings-panel-connections">

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
              <Button
                variant="outline"
                data-testid="btn-disconnect-github"
                onClick={logout}
              >
                <LogOut className="size-3.5" />
                Sign Out
              </Button>
            </div>
          ) : (
            <div className="flex items-center justify-between">
              <p className="text-sm text-muted-foreground">Not connected</p>
              <Button
                variant="outline"
                data-testid="btn-connect-github"
                onClick={() => setLoginDialogOpen(true)}
              >
                <Github className="size-3.5" />
                Sign in with GitHub
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      <GitHubLoginDialog open={loginDialogOpen} onOpenChange={setLoginDialogOpen} />
    </SettingsPanelShell>
  );
}
