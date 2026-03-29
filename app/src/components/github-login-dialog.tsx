import { useCallback, useEffect, useRef, useState } from 'react';
import { openUrl } from '@tauri-apps/plugin-opener';
import { Check, Copy, Github, Loader2, ExternalLink } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { githubStartDeviceFlow, githubPollForToken } from '@/lib/tauri';
import type { DeviceFlowResponse } from '@/lib/types';
import { useAuthStore } from '@/stores/auth-store';
import { logger } from '@/lib/logger';

interface GitHubLoginDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

type FlowState =
  | { step: 'loading' }
  | { step: 'code'; device: DeviceFlowResponse; copied: boolean; opened: boolean }
  | { step: 'polling'; userCode: string }
  | { step: 'success'; login: string }
  | { step: 'error'; message: string };

export function GitHubLoginDialog({ open, onOpenChange }: GitHubLoginDialogProps) {
  const [state, setState] = useState<FlowState>({ step: 'loading' });
  const deviceRef = useRef<DeviceFlowResponse | null>(null);
  const pollingRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const successTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const intervalRef = useRef(5);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (successTimeoutRef.current) clearTimeout(successTimeoutRef.current);
    };
  }, []);

  const stopPolling = useCallback(() => {
    if (pollingRef.current) {
      clearTimeout(pollingRef.current);
      pollingRef.current = null;
    }
  }, []);

  const startDeviceFlow = useCallback(async () => {
    setState({ step: 'loading' });
    logger.info('github: starting device flow');
    try {
      const device = await githubStartDeviceFlow();
      deviceRef.current = device;
      intervalRef.current = device.interval;
      if (mountedRef.current) {
        setState({ step: 'code', device, copied: false, opened: false });
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.error('github: device flow failed', message);
      if (mountedRef.current) {
        setState({ step: 'error', message });
      }
    }
  }, []);

  useEffect(() => {
    if (open) {
      startDeviceFlow();
    } else {
      stopPolling();
      setState({ step: 'loading' });
      deviceRef.current = null;
    }
    return () => stopPolling();
  }, [open, startDeviceFlow, stopPolling]);

  const handleCopy = async () => {
    if (state.step !== 'code') return;
    try {
      await navigator.clipboard.writeText(state.device.user_code);
      setState((prev) => (prev.step === 'code' ? { ...prev, copied: true } : prev));
      setTimeout(() => {
        setState((prev) => (prev.step === 'code' ? { ...prev, copied: false } : prev));
      }, 2000);
    } catch {
      logger.warn('github: clipboard write failed — user must copy manually');
    }
  };

  const startPolling = useCallback(() => {
    const device = deviceRef.current;
    if (!device) return;

    setState({ step: 'polling', userCode: device.user_code });

    const poll = async () => {
      if (!mountedRef.current) return;
      try {
        const result = await githubPollForToken(device.device_code);
        if (!mountedRef.current) return;

        if (result.status === 'pending') {
          pollingRef.current = setTimeout(poll, intervalRef.current * 1000);
        } else if (result.status === 'slow_down') {
          intervalRef.current += 5;
          pollingRef.current = setTimeout(poll, intervalRef.current * 1000);
        } else if (result.status === 'success') {
          logger.info(`github: signed in as ${result.user.login}`);
          useAuthStore.getState().setUser(result.user);
          setState({ step: 'success', login: result.user.login });
          successTimeoutRef.current = setTimeout(() => {
            if (mountedRef.current) onOpenChange(false);
          }, 1500);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        logger.error('github: polling failed', message);
        if (mountedRef.current) {
          setState({ step: 'error', message });
        }
      }
    };

    pollingRef.current = setTimeout(poll, intervalRef.current * 1000);
  }, [onOpenChange]);

  const handleOpenGitHub = async () => {
    if (state.step !== 'code') return;
    try {
      await openUrl(state.device.verification_uri);
      setState((prev) => (prev.step === 'code' ? { ...prev, opened: true } : prev));
      startPolling();
    } catch {
      startPolling();
    }
  };

  const handleTryAgain = () => {
    stopPolling();
    startDeviceFlow();
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-sm">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Github className="size-5" />
            Sign in with GitHub
          </DialogTitle>
          <DialogDescription>
            Authorize Migration Utility to access your GitHub account using device
            flow authentication.
          </DialogDescription>
        </DialogHeader>

        {state.step === 'loading' && (
          <div className="flex flex-col items-center justify-center gap-3 py-8">
            <Loader2 className="size-6 animate-spin text-muted-foreground" />
            <p className="text-sm text-muted-foreground">Starting authentication...</p>
          </div>
        )}

        {state.step === 'code' && (
          <div className="space-y-6 py-2">
            <div className="space-y-3 text-sm text-muted-foreground">
              <div className="flex items-start gap-2.5">
                <span className="flex size-5 shrink-0 items-center justify-center rounded-full bg-muted text-xs font-medium">
                  1
                </span>
                <span>Copy your device code</span>
              </div>
              <div className="flex items-start gap-2.5">
                <span className="flex size-5 shrink-0 items-center justify-center rounded-full bg-muted text-xs font-medium">
                  2
                </span>
                <span>Open GitHub and paste the code</span>
              </div>
              <div className="flex items-start gap-2.5">
                <span className="flex size-5 shrink-0 items-center justify-center rounded-full bg-muted text-xs font-medium">
                  3
                </span>
                <span>Authorize the application</span>
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between rounded-lg border bg-muted/50 px-4 py-3">
                <code className="font-mono text-xl font-bold tracking-widest">
                  {state.device.user_code}
                </code>
                <Button variant="ghost" size="icon" onClick={handleCopy} title="Copy code" aria-label="Copy code">
                  {state.copied ? (
                    <Check className="size-4" style={{ color: 'var(--color-seafoam)' }} />
                  ) : (
                    <Copy className="size-4" />
                  )}
                </Button>
              </div>
            </div>

            <Button className="w-full" onClick={handleOpenGitHub}>
              <ExternalLink className="size-4" />
              Open GitHub
            </Button>
          </div>
        )}

        {state.step === 'polling' && (
          <div className="flex flex-col items-center justify-center gap-4 py-8">
            <Loader2 className="size-6 animate-spin text-muted-foreground" />
            <div className="space-y-1 text-center">
              <p className="text-sm font-medium">Waiting for authorization...</p>
              <p className="text-xs text-muted-foreground">
                Enter code{' '}
                <code className="rounded bg-muted px-1 py-0.5 font-mono font-bold">
                  {state.userCode}
                </code>{' '}
                on GitHub
              </p>
            </div>
          </div>
        )}

        {state.step === 'success' && (
          <div className="flex flex-col items-center justify-center gap-3 py-8">
            <div
              className="flex size-10 items-center justify-center rounded-full"
              style={{ background: 'color-mix(in oklch, var(--color-seafoam), transparent 85%)' }}
            >
              <Check className="size-5" style={{ color: 'var(--color-seafoam)' }} />
            </div>
            <div className="space-y-1 text-center">
              <p className="text-sm font-medium">Signed in successfully</p>
              <p className="text-xs text-muted-foreground">Welcome, {state.login}</p>
            </div>
          </div>
        )}

        {state.step === 'error' && (
          <div className="space-y-4 py-4">
            <div className="rounded-lg border border-destructive/20 bg-destructive/5 p-3">
              <p className="text-sm text-destructive">{state.message}</p>
            </div>
            <Button variant="outline" className="w-full" onClick={handleTryAgain}>
              Try Again
            </Button>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
