package stream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// StorageProvider creates a storage for benchmarking
type StorageProvider struct {
	Name    string
	Factory func(b *testing.B) (StreamStorage, func())
}

var storageProviders = []StorageProvider{
	{
		Name: "Memory",
		Factory: func(b *testing.B) (StreamStorage, func()) {
			return NewMemoryStorage(), func() {}
		},
	},
	{
		Name: "Redis",
		Factory: func(b *testing.B) (StreamStorage, func()) {
			cfg := DefaultRedisConfig("localhost:6379")
			storage, err := NewRedisStorage(cfg)
			if err != nil {
				b.Skipf("Redis not available: %v", err)
				return nil, nil
			}
			cleanup := func() {
				ctx := context.Background()
				keys, _ := storage.client.Keys(ctx, "ds:*cmp*").Result()
				if len(keys) > 0 {
					storage.client.Del(ctx, keys...)
				}
				storage.Close()
			}
			return storage, cleanup
		},
	},
}

// BenchmarkStorageAppend compares append throughput across storage backends.
func BenchmarkStorageAppend(b *testing.B) {
	for _, provider := range storageProviders {
		b.Run(provider.Name, func(b *testing.B) {
			storage, cleanup := provider.Factory(b)
			if storage == nil {
				return
			}
			defer cleanup()

			ctx := context.Background()
			path := fmt.Sprintf("/cmp/append/%s", provider.Name)

			err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
			if err != nil {
				b.Fatalf("failed to create stream: %v", err)
			}

			stream, err := storage.Get(ctx, path)
			if err != nil {
				b.Fatalf("failed to get stream: %v", err)
			}

			data := make([]byte, 100)
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
			b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds()/1024/1024, "MB/s")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkStorageRead compares read throughput across storage backends.
func BenchmarkStorageRead(b *testing.B) {
	for _, provider := range storageProviders {
		b.Run(provider.Name, func(b *testing.B) {
			storage, cleanup := provider.Factory(b)
			if storage == nil {
				return
			}
			defer cleanup()

			ctx := context.Background()
			path := fmt.Sprintf("/cmp/read/%s", provider.Name)

			err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
			if err != nil {
				b.Fatalf("failed to create stream: %v", err)
			}

			stream, err := storage.Get(ctx, path)
			if err != nil {
				b.Fatalf("failed to get stream: %v", err)
			}

			// Pre-populate with 1000 messages
			for i := 0; i < 1000; i++ {
				stream.Append(ctx, make([]byte, 100))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				batch, err := stream.ReadFrom(ctx, StartOffset)
				if err != nil {
					b.Fatalf("read failed: %v", err)
				}
				if len(batch.Messages) == 0 {
					b.Fatal("expected messages")
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkStorageHead compares HEAD operation latency across storage backends.
func BenchmarkStorageHead(b *testing.B) {
	for _, provider := range storageProviders {
		b.Run(provider.Name, func(b *testing.B) {
			storage, cleanup := provider.Factory(b)
			if storage == nil {
				return
			}
			defer cleanup()

			ctx := context.Background()
			path := fmt.Sprintf("/cmp/head/%s", provider.Name)

			err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
			if err != nil {
				b.Fatalf("failed to create stream: %v", err)
			}

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
		})
	}
}

// BenchmarkStorageCreate compares stream creation latency across storage backends.
func BenchmarkStorageCreate(b *testing.B) {
	for _, provider := range storageProviders {
		b.Run(provider.Name, func(b *testing.B) {
			storage, cleanup := provider.Factory(b)
			if storage == nil {
				return
			}
			defer cleanup()

			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				path := fmt.Sprintf("/cmp/create/%s/%d", provider.Name, i)
				err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
				if err != nil {
					b.Fatalf("create failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkStorageWait compares WaitForMessages with immediate data.
func BenchmarkStorageWait(b *testing.B) {
	for _, provider := range storageProviders {
		b.Run(provider.Name, func(b *testing.B) {
			storage, cleanup := provider.Factory(b)
			if storage == nil {
				return
			}
			defer cleanup()

			ctx := context.Background()
			path := fmt.Sprintf("/cmp/wait/%s", provider.Name)

			err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
			if err != nil {
				b.Fatalf("failed to create stream: %v", err)
			}

			stream, _ := storage.Get(ctx, path)
			for i := 0; i < 100; i++ {
				stream.Append(ctx, make([]byte, 100))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := stream.WaitForMessages(ctx, StartOffset, 100*time.Millisecond)
				if result.TimedOut || len(result.Messages) == 0 {
					b.Fatal("expected immediate messages")
				}
			}
		})
	}
}

// BenchmarkStorageLargeMessages compares large message handling.
func BenchmarkStorageLargeMessages(b *testing.B) {
	sizes := []int{1024, 10240, 102400} // 1KB, 10KB, 100KB

	for _, size := range sizes {
		for _, provider := range storageProviders {
			name := fmt.Sprintf("%s/%dKB", provider.Name, size/1024)
			b.Run(name, func(b *testing.B) {
				storage, cleanup := provider.Factory(b)
				if storage == nil {
					return
				}
				defer cleanup()

				ctx := context.Background()
				path := fmt.Sprintf("/cmp/large/%s/%d", provider.Name, size)

				err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
				if err != nil {
					b.Fatalf("failed to create stream: %v", err)
				}

				stream, err := storage.Get(ctx, path)
				if err != nil {
					b.Fatalf("failed to get stream: %v", err)
				}

				data := make([]byte, size)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					_, err := stream.Append(ctx, data)
					if err != nil {
						b.Fatalf("append failed: %v", err)
					}
				}

				b.StopTimer()
				b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds()/1024/1024, "MB/s")
			})
		}
	}
}

/*
================================================================================
STORAGE COMPARISON BENCHMARK RESULTS
================================================================================

Run with: go test -bench=BenchmarkStorage -benchmem ./stream/

Actual Results (Apple M1 Pro, local Redis):

Operation       | Memory           | Redis           | Ratio
----------------|------------------|-----------------|--------
Append (100B)   | 3.3M msg/s       | 30k msg/s       | 110x
Read (1000 msg) | 32.6M msg/s      | 362k msg/s      | 90x
Head            | 4.2M ops/s       | 28k ops/s       | 150x
Create          | 1.6M ops/s       | 16k ops/s       | 100x
Wait (immed.)   | 342k ops/s       | 5k ops/s        | 68x

Large Message Throughput:
Size    | Memory     | Redis      | Ratio
--------|------------|------------|-------
1KB     | 2.4 GB/s   | 27 MB/s    | 89x
10KB    | 5.4 GB/s   | 192 MB/s   | 28x
100KB   | 6.7 GB/s   | 492 MB/s   | 14x

Key Observations:

1. MEMORY STORAGE ADVANTAGES:
   - Zero network latency (in-process)
   - No serialization overhead (direct slice access)
   - 100x+ faster for small operations
   - Suitable for: single-node, low-latency, ephemeral data

2. REDIS STORAGE ADVANTAGES:
   - Persistence across restarts
   - Shared state across processes/nodes
   - Horizontal scaling potential (Redis Cluster)
   - Built-in TTL and expiration
   - Better large message throughput ratio (14x vs 110x)
   - Suitable for: multi-node, durability required, shared state

3. PERFORMANCE TRADEOFFS:
   - Redis adds ~30-60μs per operation (network + serialization)
   - Memory storage is limited by available RAM
   - Redis can handle larger total data volumes (disk-backed)
   - Redis batching amortizes network overhead significantly
   - Large messages reduce relative Redis overhead

4. WHEN TO USE EACH:
   - Memory: Development, testing, single-process apps, in-memory cache
   - Redis: Production, distributed systems, durability requirements

5. OPTIMIZATION OPPORTUNITIES FOR REDIS:
   - Batching: AppendBatch reduces per-message overhead by 10x
   - Connection pooling: Already implemented (40 connections default)
   - Pipelining: Used in Head() and AppendBatch()
   - Large messages: Better throughput ratio (serialization amortized)

================================================================================
*/
