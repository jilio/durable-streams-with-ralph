#!/usr/bin/env bun
/**
 * Run benchmarks against a durable-streams server
 * Usage: bun scripts/run-benchmark.ts <server-url>
 */

import { spawn } from "node:child_process"
import { resolve, dirname } from "node:path"
import { fileURLToPath } from "node:url"

const __dirname = dirname(fileURLToPath(import.meta.url))
const serverUrl = process.argv[2] || "http://localhost:8080"

console.log(`\n📊 Running benchmarks against ${serverUrl}\n`)

const referenceDir = resolve(__dirname, "../reference")
const benchmarkBin = resolve(referenceDir, "node_modules/.bin/durable-streams-benchmarks")

// Check if benchmark binary exists, if not use bunx
import { existsSync } from "node:fs"

let command: string
let args: string[]

if (existsSync(benchmarkBin)) {
  command = benchmarkBin
  args = [serverUrl]
} else {
  command = "bunx"
  args = ["@durable-streams/benchmarks", serverUrl]
}

const child = spawn(command, args, {
  cwd: referenceDir,
  stdio: "inherit",
  env: {
    ...process.env,
    FORCE_COLOR: "1",
  },
})

child.on("close", (code) => {
  process.exit(code ?? 1)
})
