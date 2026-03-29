import { create } from 'zustand';
import { getSettings } from '@/lib/tauri';
import { logger } from '@/lib/logger';

interface SettingsState {
  migrationRepoFullName: string | null;
  migrationRepoCloneUrl: string | null;
  localClonePath: string | null;
  isLoading: boolean;
  loadSettings: () => Promise<void>;
}

export const useSettingsStore = create<SettingsState>()((set) => ({
  migrationRepoFullName: null,
  migrationRepoCloneUrl: null,
  localClonePath: null,
  isLoading: false,
  loadSettings: async () => {
    set({ isLoading: true });
    try {
      const s = await getSettings();
      set({
        migrationRepoFullName: s.migrationRepoFullName ?? null,
        migrationRepoCloneUrl: s.migrationRepoCloneUrl ?? null,
        localClonePath: s.localClonePath ?? null,
      });
    } catch (err) {
      logger.warn('settings-store: failed to load settings', err);
    } finally {
      set({ isLoading: false });
    }
  },
}));
