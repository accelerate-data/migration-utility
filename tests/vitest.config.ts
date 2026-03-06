import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    include: ["**/*.test.ts"],
    testTimeout: 120_000,
    hookTimeout: 30_000,
    reporters: ["default", "junit"],
    outputFile: {
      junit: "./test-results/vitest-results.xml",
    },
  },
});
