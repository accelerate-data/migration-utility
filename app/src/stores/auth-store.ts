import { create } from 'zustand';
import type { GitHubUser } from '@/lib/types';
import { githubGetUser, githubLogout } from '@/lib/tauri';
import { logger } from '@/lib/logger';

interface AuthState {
  user: GitHubUser | null;
  isLoggedIn: boolean;
  isLoading: boolean;
  lastCheckedAt: string | null;
  loadUser: () => Promise<void>;
  setUser: (user: GitHubUser | null) => void;
  logout: () => Promise<void>;
  reset: () => void;
}

const initialState = {
  user: null,
  isLoggedIn: false,
  isLoading: false,
  lastCheckedAt: null,
};

export const useAuthStore = create<AuthState>((set) => ({
  ...initialState,

  loadUser: async () => {
    set({ isLoading: true });
    try {
      const user = await githubGetUser();
      set({
        user,
        isLoggedIn: user !== null,
        isLoading: false,
        lastCheckedAt: new Date().toISOString(),
      });
    } catch {
      set({ isLoading: false, lastCheckedAt: new Date().toISOString() });
    }
  },

  setUser: (user) => {
    set({
      user,
      isLoggedIn: user !== null,
      lastCheckedAt: new Date().toISOString(),
    });
  },

  logout: async () => {
    try {
      await githubLogout();
      set({ user: null, isLoggedIn: false, lastCheckedAt: new Date().toISOString() });
      logger.info('github: signed out');
    } catch (error) {
      logger.error('github: logout failed', error);
    }
  },

  reset: () => set(initialState),
}));
