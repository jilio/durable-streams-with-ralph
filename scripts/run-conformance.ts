#!/usr/bin/env -S npx tsx
/**
 * Run conformance tests against a durable-streams server
 * Usage: bun scripts/run-conformance.ts <server-url>
 *
 * This script runs the official durable-streams conformance test suite.
 */

import { spawn } from "node:child_process"
import { resolve, dirname } from "node:path"
import { fileURLToPath } from "node:url"

const __dirname = dirname(fileURLToPath(import.meta.url))
const serverUrl = process.argv[2] || "http://localhost:8080"

console.log(`\n🧪 Running conformance tests against ${serverUrl}\n`)

const referenceDir = resolve(__dirname, "../reference")

// Create a minimal vitest config inline
// Note: longPollTimeout on reference server is 30s, so tests need > 30s timeout
const configContent = `
import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["packages/server-conformance-tests/dist/test-runner.js"],
    testTimeout: 60000,
    hookTimeout: 60000,
  },
})
`

// Write temp config
import { writeFileSync, unlinkSync } from "node:fs"
const tempConfig = resolve(referenceDir, "vitest.conformance.temp.config.ts")
writeFileSync(tempConfig, configContent)

const vitestBin = resolve(referenceDir, "node_modules/.bin/vitest")

const child = spawn(vitestBin, [
  "run",
  "--config", tempConfig,
  "--no-coverage",
  "--reporter=verbose",
], {
  cwd: referenceDir,
  stdio: "inherit",
  env: {
    ...process.env,
    CONFORMANCE_TEST_URL: serverUrl,
    FORCE_COLOR: "1",
  },
})

child.on("close", (code) => {
  try { unlinkSync(tempConfig) } catch {}
  process.exit(code ?? 1)
})
