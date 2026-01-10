package stream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkRedisAppend measures append throughput for Redis storage.
// Run with: go test -bench=BenchmarkRedisAppend -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof
func BenchmarkRedisAppend(b *testing.B) {
	storage, cleanup := setupRedisForBenchmark(b)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/bench/append"

	err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
	if err != nil {
		b.Fatalf("failed to create stream: %v", err)
	}

	stream, err := storage.Get(ctx, path)
	if err != nil {
		b.Fatalf("failed to get stream: %v", err)
	}

	data := make([]byte, 100) // 100 byte messages
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := stream.Append(ctx, data)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}

	b.StopTimer()

	// Report throughput
	b.ReportMetric(float64(b.N*len(data))/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

// BenchmarkRedisAppendBatch measures batched append throughput.
func BenchmarkRedisAppendBatch(b *testing.B) {
	storage, cleanup := setupRedisForBenchmark(b)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/bench/batch"

	err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
	if err != nil {
		b.Fatalf("failed to create stream: %v", err)
	}

	batchSizes := []int{10, 100, 1000}
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch-%d", batchSize), func(b *testing.B) {
			messages := make([][]byte, batchSize)
			for i := range messages {
				messages[i] = make([]byte, 100)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := storage.AppendBatch(ctx, path, messages)
				if err != nil {
					b.Fatalf("batch append failed: %v", err)
				}
			}

			b.StopTimer()
			totalBytes := b.N * batchSize * 100
			b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds()/1024/1024, "MB/s")
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkRedisRead measures read throughput.
func BenchmarkRedisRead(b *testing.B) {
	storage, cleanup := setupRedisForBenchmark(b)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/bench/read"

	err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
	if err != nil {
		b.Fatalf("failed to create stream: %v", err)
	}

	// Pre-populate with messages
	messages := make([][]byte, 1000)
	for i := range messages {
		messages[i] = make([]byte, 100)
	}
	_, err = storage.AppendBatch(ctx, path, messages)
	if err != nil {
		b.Fatalf("failed to populate stream: %v", err)
	}

	stream, err := storage.Get(ctx, path)
	if err != nil {
		b.Fatalf("failed to get stream: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batch, err := stream.ReadFrom(ctx, StartOffset)
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
		// Consume the batch to prevent optimization
		if len(batch.Messages) == 0 {
			b.Fatal("expected messages")
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "msg/s")
}

// BenchmarkRedisHead measures HEAD operation latency.
func BenchmarkRedisHead(b *testing.B) {
	storage, cleanup := setupRedisForBenchmark(b)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/bench/head"

	err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
	if err != nil {
		b.Fatalf("failed to create stream: %v", err)
	}

	// Add some messages
	stream, _ := storage.Get(ctx, path)
	for i := 0; i < 100; i++ {
		stream.Append(ctx, make([]byte, 100))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := storage.Head(ctx, path)
		if err != nil {
			b.Fatalf("head failed: %v", err)
		}
	}
}

// BenchmarkRedisCreate measures stream creation latency.
func BenchmarkRedisCreate(b *testing.B) {
	storage, cleanup := setupRedisForBenchmark(b)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/bench/create/%d", i)
		err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
		if err != nil {
			b.Fatalf("create failed: %v", err)
		}
	}
}

// BenchmarkRedisWaitForMessages measures WaitForMessages with immediate data.
func BenchmarkRedisWaitImmediate(b *testing.B) {
	storage, cleanup := setupRedisForBenchmark(b)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/bench/wait"

	err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
	if err != nil {
		b.Fatalf("failed to create stream: %v", err)
	}

	// Pre-populate with messages
	stream, _ := storage.Get(ctx, path)
	for i := 0; i < 100; i++ {
		stream.Append(ctx, make([]byte, 100))
	}

	stream, _ = storage.Get(ctx, path)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := stream.WaitForMessages(ctx, StartOffset, 100*time.Millisecond)
		if result.TimedOut || len(result.Messages) == 0 {
			b.Fatalf("expected immediate messages")
		}
	}
}

// BenchmarkDataEncoding measures base64 encoding/decoding overhead.
func BenchmarkDataEncoding(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				encoded := encodeData(data)
				decoded, err := decodeData(encoded)
				if err != nil {
					b.Fatal(err)
				}
				if len(decoded) != size {
					b.Fatal("size mismatch")
				}
			}
		})
	}
}

// BenchmarkKeyGeneration measures key generation overhead.
func BenchmarkKeyGeneration(b *testing.B) {
	paths := []string{
		"/test",
		"/v1/stream/events",
		"/api/v2/users/123/notifications",
	}

	for _, path := range paths {
		b.Run(fmt.Sprintf("path-%d", len(path)), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_ = streamKey(path)
				_ = metaKey(path)
				_ = seqKey(path)
			}
		})
	}
}

// BenchmarkParseRedisID measures Redis ID parsing overhead.
func BenchmarkParseRedisID(b *testing.B) {
	id := "1704067200000-42"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := parseRedisID(id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// setupRedisForBenchmark creates a Redis storage for benchmarking.
// Returns nil if Redis is not available (skips the benchmark).
func setupRedisForBenchmark(b *testing.B) (*RedisStorage, func()) {
	b.Helper()

	// Try to connect to Redis - skip if not available
	cfg := DefaultRedisConfig("localhost:6379")
	storage, err := NewRedisStorage(cfg)
	if err != nil {
		b.Skipf("Redis not available: %v", err)
		return nil, nil
	}

	// Cleanup function
	cleanup := func() {
		ctx := context.Background()
		// Clean up benchmark keys
		keys, _ := storage.client.Keys(ctx, "ds:*bench*").Result()
		if len(keys) > 0 {
			storage.client.Del(ctx, keys...)
		}
		storage.Close()
	}

	return storage, cleanup
}

/*
ProfilingNotes documents the profiling approach and results.

Run benchmarks with profiling:

    go test -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof ./stream/
    go tool pprof cpu.prof
    go tool pprof mem.prof

Key profiling commands:
    (pprof) top10           # Show top CPU/memory consumers
    (pprof) list FuncName   # Show annotated source
    (pprof) web             # Generate SVG flamegraph

================================================================================
PROFILING RESULTS SUMMARY
================================================================================

CPU Profile Analysis:
---------------------
- 29.06% runtime.kevent (network I/O wait - expected for Redis)
- 10.94% syscall.syscall (Redis network operations)
- 10.53% net/url.escape (key generation - encodePathForRedis)
-  3.00% encoding/base64.Decode (decodeData)
-  2.12% encoding/base64.Encode (encodeData)
-  1.76% runtime.scanobject (GC overhead)

Memory Profile Analysis:
------------------------
- 48.76% encoding/base64.EncodeToString (6573 MB) - encodeData
- 18.56% encoding/base64.DecodeString (2501 MB) - decodeData
-  6.88% net/url.escape (928 MB) - encodePathForRedis
-  5.50% go-redis stringInterfaceMapParser (742 MB) - XADD values
-  5.09% streamKey (687 MB) - key generation

================================================================================
HOT PATHS IDENTIFIED
================================================================================

1. BASE64 ENCODING (67% of allocations)
   - encodeData/decodeData allocate 9 GB+ during benchmarks
   - CPU: ~5% of total time
   - Every message encode/decode creates new string allocations

   Optimization options:
   a) Use binary-safe storage (Redis supports raw bytes in XADD)
   b) Pool byte buffers for encoding/decoding
   c) Store raw bytes if go-redis supports it

2. URL ESCAPE FOR KEY GENERATION (7% of allocations)
   - Called on every Redis operation (streamKey, metaKey, seqKey)
   - 928 MB allocated during benchmarks
   - Creates new strings each time

   Optimization options:
   a) Cache encoded paths in a sync.Map
   b) Pre-compute keys for frequently accessed streams
   c) Use path hash instead of URL encoding

3. REDIS STRING-INTERFACE MAP (5.5% of allocations)
   - go-redis parses XADD values into map[string]interface{}
   - Unavoidable without custom parser

4. XRANGE FULL SCAN IN ReadFrom
   - Scans entire stream on each read
   - O(n) complexity where n = total messages

   Optimization options:
   a) Use XREAD with Redis ID tracking
   b) Maintain offset → Redis ID mapping
   c) Add pagination to limit scan size

5. OFFSET GENERATOR INITIALIZATION
   - CurrentOffset() calls XRevRangeN on every Append if offsetGen is nil
   - Adds extra round-trip per append

   Optimization: Already addressed - offsetGen is cached per stream instance

================================================================================
BENCHMARK RESULTS
================================================================================

Operation         | Throughput      | Latency    | Allocs/op
------------------|-----------------|------------|----------
Append            | 2.81 MB/s       | 34 μs      | 21
Batch (10 msg)    | 6.05 MB/s       | 158 μs     | 228
Batch (100 msg)   | 25.2 MB/s       | 378 μs     | 1671
Batch (1000 msg)  | 30.8 MB/s       | 3.1 ms     | 16077
Read (1000 msg)   | 318k msg/s      | 3.1 ms     | 14042
Head              | 27.4k ops/s     | 36.5 μs    | 45
Create            | 15.7k ops/s     | 63.5 μs    | 31
WaitImmediate     | 4.9k ops/s      | 205 μs     | 1441

================================================================================
RECOMMENDATIONS
================================================================================

Priority 1 (High Impact):
- Consider raw binary storage instead of base64 if protocol allows
- Cache URL-encoded paths for hot streams

Priority 2 (Medium Impact):
- Use XREAD with COUNT for paginated reads
- Implement offset → Redis ID mapping cache

Priority 3 (Future):
- Connection pool tuning based on workload
- Benchmark Redis Cluster vs single node
*/
