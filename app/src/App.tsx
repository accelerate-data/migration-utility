import { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { ThemeProvider } from 'next-themes';
import IconNav from './components/icon-nav';
import HomeSurface from './routes/home';
import SplashScreen from './routes/splash';
import SettingsSurface from './routes/settings/index';
import { Toaster } from './components/ui/sonner';
import { useAuthStore } from './stores/auth-store';
import { useProjectStore } from './stores/project-store';
import { logger } from './lib/logger';

export default function App() {
  const loadUser = useAuthStore((s) => s.loadUser);
  const { loadProjects } = useProjectStore();

  // null = not yet determined, true = show splash, false = show app
  const [showSplash, setShowSplash] = useState<boolean | null>(null);

  useEffect(() => {
    async function bootstrap() {
      await loadUser();
      await loadProjects();
      // After loadProjects, activeProject is populated if one is set.
      // We read from the store directly after the await.
      const active = useProjectStore.getState().activeProject;
      if (active) {
        logger.debug('app: active project on startup', active.id);
        setShowSplash(true);
      } else {
        setShowSplash(false);
      }
    }
    void bootstrap();
  }, [loadUser, loadProjects]);

  if (showSplash === null) {
    // Still determining startup state — render nothing (brief flicker prevention).
    return null;
  }

  if (showSplash) {
    const active = useProjectStore.getState().activeProject!;
    return (
      <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
        <SplashScreen
          projectId={active.id}
          projectName={active.name}
          onSuccess={() => setShowSplash(false)}
          onCancel={() => {
            useProjectStore.getState().clearActive();
            setShowSplash(false);
          }}
        />
        <Toaster />
      </ThemeProvider>
    );
  }

  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <BrowserRouter>
        <div className="flex h-screen bg-background overflow-hidden">
          <IconNav />
          <main className="flex-1 overflow-hidden">
            <Routes>
              <Route path="/" element={<Navigate to="/home" replace />} />
              <Route path="/home" element={<HomeSurface />} />
              <Route path="/settings/*" element={<SettingsSurface />} />
            </Routes>
          </main>
        </div>
        <Toaster />
      </BrowserRouter>
    </ThemeProvider>
  );
}
