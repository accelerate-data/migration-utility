import { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { ThemeProvider } from 'next-themes';
import IconNav from './components/icon-nav';
import HomeSurface from './routes/home';
import SettingsSurface from './routes/settings/index';
import { Toaster } from './components/ui/sonner';
import { useAuthStore } from './stores/auth-store';

export default function App() {
  const loadUser = useAuthStore((s) => s.loadUser);

  useEffect(() => {
    void loadUser();
  }, [loadUser]);

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
