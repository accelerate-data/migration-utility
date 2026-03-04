import { useEffect, type ReactNode } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { ThemeProvider } from 'next-themes';
import IconNav from './components/icon-nav';
import HomeSurface from './routes/home';
import SettingsSurface from './routes/settings/index';
import { Toaster } from './components/ui/sonner';
import { useAuthStore } from './stores/auth-store';
import {
  defaultRouteForPhase,
  isSurfaceEnabledForPhase,
  type Surface,
  useWorkflowStore,
} from './stores/workflow-store';
import { appHydratePhase, workspaceGet } from './lib/tauri';
import { logger } from './lib/logger';

function RootRedirect() {
  const appPhase = useWorkflowStore((s) => s.appPhase);
  const appPhaseHydrated = useWorkflowStore((s) => s.appPhaseHydrated);
  if (!appPhaseHydrated) return null;
  return <Navigate to={defaultRouteForPhase(appPhase)} replace />;
}

function GuardedRoute({ surface, element }: { surface: Surface; element: ReactNode }) {
  const appPhase = useWorkflowStore((s) => s.appPhase);
  const appPhaseHydrated = useWorkflowStore((s) => s.appPhaseHydrated);

  if (!appPhaseHydrated) {
    return (
      <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
        Loading workspace state...
      </div>
    );
  }

  if (!isSurfaceEnabledForPhase(surface, appPhase)) {
    return <Navigate to={defaultRouteForPhase(appPhase)} replace />;
  }

  return <>{element}</>;
}

export default function App() {
  const loadUser = useAuthStore((s) => s.loadUser);
  const setWorkspaceId = useWorkflowStore((s) => s.setWorkspaceId);
  const clearWorkspaceId = useWorkflowStore((s) => s.clearWorkspaceId);
  const setAppPhaseState = useWorkflowStore((s) => s.setAppPhaseState);
  const setAppPhaseHydrated = useWorkflowStore((s) => s.setAppPhaseHydrated);

  useEffect(() => {
    void loadUser();
  }, [loadUser]);

  useEffect(() => {
    let mounted = true;
    async function bootstrap() {
      try {
        const [workspace, phase] = await Promise.all([workspaceGet(), appHydratePhase()]);
        if (!mounted) return;
        if (workspace?.id) {
          setWorkspaceId(workspace.id);
        } else {
          clearWorkspaceId();
        }
        setAppPhaseState(phase);
      } catch (err) {
        logger.error('app bootstrap failed', err);
        if (!mounted) return;
        setAppPhaseHydrated(true);
      }
    }

    void bootstrap();
    return () => {
      mounted = false;
    };
  }, [clearWorkspaceId, setAppPhaseHydrated, setAppPhaseState, setWorkspaceId]);

  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <BrowserRouter>
        <div className="flex h-screen bg-background overflow-hidden">
          <IconNav />
          <main className="flex-1 overflow-hidden">
            <Routes>
              <Route path="/" element={<RootRedirect />} />
              <Route path="/home" element={<GuardedRoute surface="home" element={<HomeSurface />} />} />
              <Route path="/settings/*" element={<GuardedRoute surface="settings" element={<SettingsSurface />} />} />
            </Routes>
          </main>
        </div>
        <Toaster />
      </BrowserRouter>
    </ThemeProvider>
  );
}
