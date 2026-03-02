import { createAbortState, handleShutdown } from "./shutdown.ts";
import { runPersistent } from "./persistent-mode.ts";

const state = createAbortState();

process.on("SIGTERM", () => handleShutdown(state));
process.on("SIGINT", () => handleShutdown(state));

process.on("uncaughtException", (err) => {
  process.stderr.write(`[sidecar] Uncaught exception: ${err.stack || err.message}\n`);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  const msg = reason instanceof Error ? (reason.stack || reason.message) : String(reason);
  process.stderr.write(`[sidecar] Unhandled rejection: ${msg}\n`);
  process.exit(1);
});

runPersistent()
  .then(() => {
    process.stderr.write("[sidecar] exited\n");
    process.exit(0);
  })
  .catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[sidecar] fatal: ${message}\n`);
    process.exit(1);
  });
