import { useRef } from 'react';
import { listenProjectInitStep, projectInit } from '@/lib/tauri';
import { useProjectStore } from '@/stores/project-store';

/**
 * Encapsulates the project-init lifecycle: subscribe to init-step events,
 * run projectInit, and clean up the listener when done.
 */
export function useProjectInit() {
  const { startInit, finishInit, applyInitStep } = useProjectStore();
  const unlistenRef = useRef<(() => void) | null>(null);

  async function runInit(projectId: string): Promise<void> {
    startInit();
    try {
      unlistenRef.current = await listenProjectInitStep((ev) => applyInitStep(ev));
      // Yield one tick to ensure the listener is fully registered before events start.
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      await projectInit(projectId);
      finishInit();
    } catch (err) {
      finishInit();
      throw err;
    } finally {
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  return { runInit };
}
