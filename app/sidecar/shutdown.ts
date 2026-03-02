export interface AbortState {
  abortController: AbortController;
}

/**
 * Create a fresh abort state for shutdown handling.
 */
export function createAbortState(): AbortState {
  return {
    abortController: new AbortController(),
  };
}

/**
 * Link an external AbortSignal to an internal AbortState.
 * If the external signal is already aborted, aborts immediately.
 * Otherwise, listens for the abort event and forwards it.
 */
export function linkExternalSignal(
  state: AbortState,
  signal: AbortSignal,
): void {
  if (signal.aborted) {
    state.abortController.abort();
  } else {
    signal.addEventListener(
      "abort",
      () => {
        state.abortController.abort();
      },
      { once: true },
    );
  }
}

/**
 * Handle a shutdown signal (SIGTERM / SIGINT).
 * Sets the aborted flag, calls abort(), and schedules a force exit.
 *
 * @param state   The abort state to mutate
 * @param exitFn  Function to call for force-exit (defaults to process.exit)
 * @param timerFn Function to schedule the force-exit (defaults to setTimeout)
 */
export function handleShutdown(
  state: AbortState,
  exitFn: (code: number) => void = (code) => process.exit(code),
  timerFn: (cb: () => void, ms: number) => { unref(): void } = (cb, ms) => setTimeout(cb, ms),
) {
  state.abortController.abort();
  // Force exit after 3s if SDK doesn't respond to abort
  timerFn(() => exitFn(0), 3000).unref();
}
