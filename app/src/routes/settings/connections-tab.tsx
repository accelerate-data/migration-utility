import { useEffect, useState } from 'react';
import { CheckCircle2, Github, Loader2, LogOut } from 'lucide-react';
import { toast } from 'sonner';
import { useAuthStore } from '@/stores/auth-store';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { GitHubLoginDialog } from '@/components/github-login-dialog';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import { getSettings, listModels, saveAgentSettings, saveAnthropicApiKey, testApiKey } from '@/lib/tauri';
import { logger } from '@/lib/logger';

const EFFORT_OPTIONS = ['low', 'medium', 'high', 'max'] as const;

export default function ConnectionsTab() {
  const { user, isLoggedIn, isLoading: isAuthLoading, lastCheckedAt, loadUser, logout } = useAuthStore();
  const [loginDialogOpen, setLoginDialogOpen] = useState(false);
  const [apiKey, setApiKey] = useState('');
  const [testingApiKey, setTestingApiKey] = useState(false);
  const [apiKeyValid, setApiKeyValid] = useState<boolean | null>(null);

  const [preferredModel, setPreferredModel] = useState<string | null>(null);
  const [availableModels, setAvailableModels] = useState<{ id: string; displayName: string }[]>([]);
  const [effort, setEffort] = useState<string>('high');

  useEffect(() => {
    loadUser();
  }, [loadUser]);

  useEffect(() => {
    getSettings()
      .then((settings) => {
        // anthropicApiKey is not returned by the backend (security) — input stays empty
        setPreferredModel(settings.preferredModel ?? null);
        setEffort(settings.effort ?? 'high');
      })
      .catch((err) => {
        logger.error('get_settings failed', err);
      });
  }, []);

  useEffect(() => {
    if (!apiKey) return;
    listModels(apiKey)
      .then(setAvailableModels)
      .catch(() => {
        // silently ignore — models list stays empty, placeholder shown
      });
  }, [apiKey]);

  async function handleSaveApiKey(nextValue: string) {
    try {
      await saveAnthropicApiKey(nextValue.trim() ? nextValue.trim() : null);
      toast.success(nextValue.trim() ? 'API key saved' : 'API key cleared');
      logger.info('settings: anthropic API key saved');
    } catch (err) {
      logger.error('save_anthropic_api_key failed', err);
      toast.error('Failed to save API key');
    }
  }

  async function handleTestApiKey() {
    const key = apiKey.trim();
    if (!key) {
      toast.error('Enter an API key first');
      return;
    }
    setTestingApiKey(true);
    setApiKeyValid(null);
    try {
      await testApiKey(key);
      setApiKeyValid(true);
      toast.success('API key is valid');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.error('test_api_key failed', err);
      setApiKeyValid(false);
      toast.error(message);
    } finally {
      setTestingApiKey(false);
    }
  }

  async function handleSaveAgentSettings(model: string | null, eff: string) {
    try {
      await saveAgentSettings(model, eff);
      logger.info('settings: agent settings saved model=%s effort=%s', model, eff);
    } catch (err) {
      logger.error('save_agent_settings failed', err);
      toast.error('Failed to save agent settings');
    }
  }

  const githubStatus = isAuthLoading ? 'Checking' : isLoggedIn && user ? 'Connected' : 'Not connected';

  return (
    <SettingsPanelShell
      panelTestId="settings-panel-connections"
    >

        {/* GitHub */}
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

        {/* Anthropic API key */}
        <Card className="gap-0 py-5" data-testid="settings-connections-anthropic-card">
          <CardHeader className="pb-3">
            <CardTitle>Anthropic API key</CardTitle>
            <CardDescription className="mt-0.5">
              Used by the headless pipeline agents during migration execution.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex gap-2 items-center">
            <Input
              id="anthropic-key"
              data-testid="input-anthropic-key"
              type="password"
              value={apiKey}
              onChange={(e) => {
                setApiKey(e.target.value);
                setApiKeyValid(null);
              }}
              onBlur={() => {
                void handleSaveApiKey(apiKey);
              }}
              placeholder="sk-ant-api03-…"
              className="font-mono text-sm flex-1"
            />
            <Button
              type="button"
              variant={apiKeyValid ? 'default' : 'outline'}
              data-testid="btn-update-anthropic-key"
              onClick={() => {
                void handleTestApiKey();
              }}
              disabled={testingApiKey || !apiKey.trim()}
              className={apiKeyValid ? 'text-white' : undefined}
              style={apiKeyValid ? { background: 'var(--color-seafoam)', color: 'white' } : undefined}
            >
              {testingApiKey ? <Loader2 className="size-3.5 animate-spin" /> : null}
              {!testingApiKey && apiKeyValid ? <CheckCircle2 className="size-3.5" /> : null}
              {apiKeyValid ? 'Valid' : 'Test'}
            </Button>
          </CardContent>
        </Card>

        {/* Agent settings */}
        <Card className="gap-0 py-5" data-testid="settings-connections-agent-card">
          <CardHeader className="pb-3">
            <CardTitle>Agent</CardTitle>
            <CardDescription className="mt-0.5">
              Runtime settings for analysis agents. Model in agent front-matter takes precedence.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0 flex flex-col gap-5">

            {/* Model */}
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs font-medium text-muted-foreground">Fallback model</Label>
              <Select
                value={preferredModel ?? ''}
                onValueChange={(value) => {
                  const next = value || null;
                  setPreferredModel(next);
                  void handleSaveAgentSettings(next, effort);
                }}
                disabled={availableModels.length === 0}
              >
                <SelectTrigger className="w-72" data-testid="select-preferred-model">
                  <SelectValue placeholder="Default (claude-sonnet-4-6)" />
                </SelectTrigger>
                <SelectContent>
                  {availableModels.map((m) => (
                    <SelectItem key={m.id} value={m.id}>
                      {m.displayName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {availableModels.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  Enter your API key above to load available models.
                </p>
              )}
            </div>

            {/* Effort */}
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs font-medium text-muted-foreground">Effort</Label>
              <RadioGroup
                value={effort}
                onValueChange={(value) => {
                  setEffort(value);
                  void handleSaveAgentSettings(preferredModel, value);
                }}
                className="flex gap-1"
                data-testid="radio-effort"
              >
                {EFFORT_OPTIONS.map((opt) => (
                  <div key={opt} className="flex items-center">
                    <RadioGroupItem value={opt} id={`effort-${opt}`} className="sr-only" />
                    <Label
                      htmlFor={`effort-${opt}`}
                      className={[
                        'cursor-pointer rounded-md border px-3 py-1.5 text-xs font-medium transition-colors duration-150',
                        effort === opt
                          ? 'border-transparent text-white'
                          : 'border-border bg-background text-muted-foreground hover:bg-muted',
                        '',
                      ].join(' ')}
                      style={effort === opt ? { background: 'var(--color-pacific)' } : undefined}
                    >
                      {opt.charAt(0).toUpperCase() + opt.slice(1)}
                    </Label>
                  </div>
                ))}
              </RadioGroup>
            </div>

          </CardContent>
        </Card>

      <GitHubLoginDialog open={loginDialogOpen} onOpenChange={setLoginDialogOpen} />
    </SettingsPanelShell>
  );
}
