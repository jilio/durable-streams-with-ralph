#!/usr/bin/env bun
/**
 * Run simple throughput benchmarks against a durable-streams server
 * Usage: bun scripts/run-benchmark.ts <server-url>
 */

const serverUrl = process.argv[2] || "http://localhost:8080"

console.log(`\n📊 Running benchmarks against ${serverUrl}\n`)

interface BenchResult {
  name: string
  value: number
  unit: string
}

const results: BenchResult[] = []

async function runBenchmark() {
  // 1. Baseline latency - simple HEAD request
  console.log("📍 Testing baseline latency...")
  const latencies: number[] = []
  for (let i = 0; i < 100; i++) {
    const start = performance.now()
    await fetch(`${serverUrl}/health`).catch(() => {})
    latencies.push(performance.now() - start)
  }
  const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length
  results.push({ name: "Baseline Latency (avg)", value: avgLatency, unit: "ms" })
  console.log(`   Average: ${avgLatency.toFixed(2)}ms`)

  // 2. Create stream
  const streamPath = `/v1/stream/benchmark-${Date.now()}`
  await fetch(`${serverUrl}${streamPath}`, {
    method: "PUT",
    headers: { "Content-Type": "application/octet-stream" },
    body: "init",
  })

  // 3. Small message throughput (100 bytes)
  console.log("📍 Testing small message throughput (100 bytes x 1000)...")
  const smallMsg = new Uint8Array(100).fill(42)
  const smallStart = performance.now()
  const smallPromises = []
  for (let i = 0; i < 1000; i++) {
    smallPromises.push(
      fetch(`${serverUrl}${streamPath}`, {
        method: "POST",
        headers: { "Content-Type": "application/octet-stream" },
        body: smallMsg,
      })
    )
  }
  await Promise.all(smallPromises)
  const smallElapsed = (performance.now() - smallStart) / 1000
  const smallThroughput = 1000 / smallElapsed
  results.push({ name: "Small Message Throughput", value: smallThroughput, unit: "msg/sec" })
  console.log(`   ${smallThroughput.toFixed(0)} msg/sec`)

  // 4. Large message throughput (1MB)
  console.log("📍 Testing large message throughput (1MB x 20)...")
  const streamPath2 = `/v1/stream/benchmark-large-${Date.now()}`
  await fetch(`${serverUrl}${streamPath2}`, {
    method: "PUT",
    headers: { "Content-Type": "application/octet-stream" },
    body: "init",
  })

  const largeMsg = new Uint8Array(1024 * 1024).fill(42)
  const largeStart = performance.now()
  for (let i = 0; i < 20; i++) {
    await fetch(`${serverUrl}${streamPath2}`, {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream" },
      body: largeMsg,
    })
  }
  const largeElapsed = (performance.now() - largeStart) / 1000
  const largeThroughput = 20 / largeElapsed
  const byteThroughput = (20 * 1024 * 1024) / largeElapsed / (1024 * 1024)
  results.push({ name: "Large Message Throughput", value: largeThroughput, unit: "msg/sec" })
  results.push({ name: "Byte Throughput (write)", value: byteThroughput, unit: "MB/sec" })
  console.log(`   ${largeThroughput.toFixed(2)} msg/sec (${byteThroughput.toFixed(2)} MB/sec)`)

  // 5. Read throughput
  console.log("📍 Testing read throughput...")
  const readStart = performance.now()
  const readRes = await fetch(`${serverUrl}${streamPath2}?offset=-1`)
  const readBody = await readRes.arrayBuffer()
  const readElapsed = (performance.now() - readStart) / 1000
  const readThroughput = readBody.byteLength / readElapsed / (1024 * 1024)
  results.push({ name: "Byte Throughput (read)", value: readThroughput, unit: "MB/sec" })
  console.log(`   ${readThroughput.toFixed(2)} MB/sec (${(readBody.byteLength / 1024 / 1024).toFixed(2)} MB)`)

  // Cleanup
  await fetch(`${serverUrl}${streamPath}`, { method: "DELETE" })
  await fetch(`${serverUrl}${streamPath2}`, { method: "DELETE" })

  // Print summary
  console.log("\n=== BENCHMARK RESULTS ===")
  console.table(
    results.reduce((acc, r) => {
      acc[r.name] = `${r.value.toFixed(2)} ${r.unit}`
      return acc
    }, {} as Record<string, string>)
  )
}

runBenchmark().catch(console.error)
