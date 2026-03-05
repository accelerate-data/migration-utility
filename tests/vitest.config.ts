import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    globalSetup: "./global-setup.ts",
    // Each test spawns a real claude CLI invocation — allow up to 5 minutes
    testTimeout: 300_000,
    hookTimeout: 30_000,
    include: ["**/*.test.ts"],
    reporters: ["verbose"],
    outputFile: {
      junit: "../test-results/integration-results.xml",
    },
  },
});
