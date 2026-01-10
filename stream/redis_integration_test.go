package stream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Redis integration tests - require a running Redis instance.
// These tests are skipped if Redis is not available.
// Run with: go test -v ./stream/ -run TestRedisIntegration
// Or with a specific Redis: REDIS_ADDR=localhost:6379 go test -v ./stream/ -run TestRedisIntegration

// getRedisStorage creates a Redis storage for integration testing.
// Returns nil and skips the test if Redis is not available.
func getRedisStorage(t *testing.T) (*RedisStorage, func()) {
	t.Helper()

	cfg := DefaultRedisConfig("localhost:6379")
	storage, err := NewRedisStorage(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
		return nil, nil
	}

	cleanup := func() {
		ctx := context.Background()
		// Clean up test keys
		keys, _ := storage.client.Keys(ctx, "ds:*integration*").Result()
		if len(keys) > 0 {
			storage.client.Del(ctx, keys...)
		}
		storage.Close()
	}

	return storage, cleanup
}

func TestRedisIntegration_CreateAndGet(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/create-get"

	// Create stream
	err := storage.Create(ctx, StreamConfig{
		Path:        path,
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get stream
	stream, err := storage.Get(ctx, path)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if stream.Path() != path {
		t.Errorf("Path = %q, want %q", stream.Path(), path)
	}
	if stream.ContentType() != "application/json" {
		t.Errorf("ContentType = %q, want %q", stream.ContentType(), "application/json")
	}
}

func TestRedisIntegration_CreateIdempotent(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/idempotent"
	config := StreamConfig{Path: path, ContentType: "text/plain"}

	// First create
	err := storage.Create(ctx, config)
	if err != nil {
		t.Fatalf("First Create failed: %v", err)
	}

	// Second create with same config should return ErrStreamExists
	err = storage.Create(ctx, config)
	if err != ErrStreamExists {
		t.Errorf("Second Create = %v, want ErrStreamExists", err)
	}

	// Create with different config should return ErrConfigMismatch
	err = storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/json"})
	if err != ErrConfigMismatch {
		t.Errorf("Create with different config = %v, want ErrConfigMismatch", err)
	}
}

func TestRedisIntegration_Delete(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/delete"

	// Create stream
	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Delete stream
	err = storage.Delete(ctx, path)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	exists, err := storage.Exists(ctx, path)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("Stream still exists after delete")
	}

	// Delete non-existent should return ErrStreamNotFound
	err = storage.Delete(ctx, path)
	if err != ErrStreamNotFound {
		t.Errorf("Delete non-existent = %v, want ErrStreamNotFound", err)
	}
}

func TestRedisIntegration_Head(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/head"

	// Create stream
	err := storage.Create(ctx, StreamConfig{Path: path, ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Head on empty stream
	meta, err := storage.Head(ctx, path)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if meta.Path != path {
		t.Errorf("Path = %q, want %q", meta.Path, path)
	}

	// Append some messages
	stream, _ := storage.Get(ctx, path)
	for i := 0; i < 5; i++ {
		stream.Append(ctx, []byte(fmt.Sprintf("message-%d", i)))
	}

	// Head should reflect new offset
	meta, err = storage.Head(ctx, path)
	if err != nil {
		t.Fatalf("Head after append failed: %v", err)
	}
	if meta.NextOffset == "0_0" {
		t.Error("NextOffset should have changed after append")
	}
}

func TestRedisIntegration_Append(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/append"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	stream, _ := storage.Get(ctx, path)

	// Append messages
	offsets := make([]Offset, 10)
	for i := 0; i < 10; i++ {
		offset, err := stream.Append(ctx, []byte(fmt.Sprintf("message-%d", i)))
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
		offsets[i] = offset
	}

	// Verify offsets are strictly increasing
	for i := 1; i < len(offsets); i++ {
		if offsets[i].Compare(offsets[i-1]) <= 0 {
			t.Errorf("Offset %d (%s) not greater than offset %d (%s)",
				i, offsets[i], i-1, offsets[i-1])
		}
	}
}

func TestRedisIntegration_AppendBatch(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/batch"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Batch append
	messages := make([][]byte, 100)
	for i := range messages {
		messages[i] = []byte(fmt.Sprintf("batch-message-%d", i))
	}

	offsets, err := storage.AppendBatch(ctx, path, messages)
	if err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	if len(offsets) != len(messages) {
		t.Errorf("Got %d offsets, want %d", len(offsets), len(messages))
	}

	// Verify all messages readable
	stream, _ := storage.Get(ctx, path)
	batch, err := stream.ReadFrom(ctx, StartOffset)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if len(batch.Messages) != len(messages) {
		t.Errorf("Read %d messages, want %d", len(batch.Messages), len(messages))
	}
}

func TestRedisIntegration_ReadFrom(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/read"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	stream, _ := storage.Get(ctx, path)

	// Append messages
	var midOffset Offset
	for i := 0; i < 10; i++ {
		offset, _ := stream.Append(ctx, []byte(fmt.Sprintf("message-%d", i)))
		if i == 4 {
			midOffset = offset
		}
	}

	// Read all from start
	batch, err := stream.ReadFrom(ctx, StartOffset)
	if err != nil {
		t.Fatalf("ReadFrom start failed: %v", err)
	}
	if len(batch.Messages) != 10 {
		t.Errorf("Read %d messages from start, want 10", len(batch.Messages))
	}

	// Read from middle offset
	batch, err = stream.ReadFrom(ctx, midOffset)
	if err != nil {
		t.Fatalf("ReadFrom middle failed: %v", err)
	}
	if len(batch.Messages) != 5 {
		t.Errorf("Read %d messages from middle, want 5", len(batch.Messages))
	}

	// Verify first message is after midOffset
	if batch.Messages[0].Offset.Compare(midOffset) <= 0 {
		t.Error("First message should be after midOffset")
	}
}

func TestRedisIntegration_ReadFromBinaryData(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/binary"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	stream, _ := storage.Get(ctx, path)

	// Test various binary data patterns
	testCases := []struct {
		name string
		data []byte
	}{
		{"null bytes", []byte{0x00, 0x00, 0x00}},
		{"all 0xFF", []byte{0xFF, 0xFF, 0xFF}},
		{"mixed binary", []byte{0x00, 0x7F, 0x80, 0xFF, 0x01}},
		{"unicode", []byte("こんにちは世界")},
		{"empty", []byte{}},
		{"large binary", make([]byte, 10000)},
	}

	// Append all test data
	for _, tc := range testCases {
		_, err := stream.Append(ctx, tc.data)
		if err != nil {
			t.Fatalf("Append %s failed: %v", tc.name, err)
		}
	}

	// Read back and verify
	batch, _ := stream.ReadFrom(ctx, StartOffset)
	for i, tc := range testCases {
		if i >= len(batch.Messages) {
			t.Fatalf("Missing message %d (%s)", i, tc.name)
		}
		got := batch.Messages[i].Data
		if string(got) != string(tc.data) {
			t.Errorf("%s: data mismatch, got %d bytes, want %d bytes",
				tc.name, len(got), len(tc.data))
		}
	}
}

func TestRedisIntegration_WaitForMessages(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/wait"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	stream, _ := storage.Get(ctx, path)

	// Test 1: Wait with existing data should return immediately
	stream.Append(ctx, []byte("existing"))
	result := stream.WaitForMessages(ctx, StartOffset, 100*time.Millisecond)
	if result.TimedOut {
		t.Error("Should not timeout when data exists")
	}
	if len(result.Messages) == 0 {
		t.Error("Should return existing messages")
	}

	// Test 2: Wait with no new data should timeout
	currentOffset := stream.CurrentOffset()
	result = stream.WaitForMessages(ctx, currentOffset, 100*time.Millisecond)
	if !result.TimedOut {
		t.Error("Should timeout when no new data")
	}

	// Test 3: Wait receives data written by another goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		stream.Append(ctx, []byte("async message"))
	}()

	result = stream.WaitForMessages(ctx, currentOffset, 500*time.Millisecond)
	wg.Wait()

	if result.TimedOut {
		t.Error("Should receive async message")
	}
	if len(result.Messages) == 0 {
		t.Error("Should have received async message")
	}
}

func TestRedisIntegration_AppendWithProducer(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/producer"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	stream, _ := storage.Get(ctx, path)
	producerID := "test-producer"

	// First message (epoch=1, seq=0)
	offset, result := stream.AppendWithProducer(ctx, []byte("msg-0"), producerID, 1, 0)
	if result.Status != ProducerStatusAccepted {
		t.Errorf("First message status = %v, want Accepted", result.Status)
	}
	if offset == "" {
		t.Error("First message should return offset")
	}

	// Second message (epoch=1, seq=1)
	_, result = stream.AppendWithProducer(ctx, []byte("msg-1"), producerID, 1, 1)
	if result.Status != ProducerStatusAccepted {
		t.Errorf("Second message status = %v, want Accepted", result.Status)
	}

	// Duplicate (epoch=1, seq=1)
	_, result = stream.AppendWithProducer(ctx, []byte("msg-1-dup"), producerID, 1, 1)
	if result.Status != ProducerStatusDuplicate {
		t.Errorf("Duplicate status = %v, want Duplicate", result.Status)
	}

	// Gap (epoch=1, seq=5)
	_, result = stream.AppendWithProducer(ctx, []byte("msg-5"), producerID, 1, 5)
	if result.Status != ProducerStatusSequenceGap {
		t.Errorf("Gap status = %v, want SequenceGap", result.Status)
	}

	// Stale epoch (epoch=0, seq=0)
	_, result = stream.AppendWithProducer(ctx, []byte("old"), producerID, 0, 0)
	if result.Status != ProducerStatusStaleEpoch {
		t.Errorf("Stale epoch status = %v, want StaleEpoch", result.Status)
	}

	// New epoch (epoch=2, seq=0)
	_, result = stream.AppendWithProducer(ctx, []byte("new-epoch"), producerID, 2, 0)
	if result.Status != ProducerStatusAccepted {
		t.Errorf("New epoch status = %v, want Accepted", result.Status)
	}
}

func TestRedisIntegration_ConcurrentAppend(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/concurrent"

	err := storage.Create(ctx, StreamConfig{Path: path})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	stream, _ := storage.Get(ctx, path)

	// Concurrent appends from multiple goroutines
	numGoroutines := 10
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*messagesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < messagesPerGoroutine; i++ {
				data := []byte(fmt.Sprintf("g%d-m%d", goroutineID, i))
				_, err := stream.Append(ctx, data)
				if err != nil {
					errChan <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("Concurrent append error: %v", err)
	}

	// Verify all messages
	batch, _ := stream.ReadFrom(ctx, StartOffset)
	expected := numGoroutines * messagesPerGoroutine
	if len(batch.Messages) != expected {
		t.Errorf("Got %d messages, want %d", len(batch.Messages), expected)
	}
}

func TestRedisIntegration_PoolStats(t *testing.T) {
	storage, cleanup := getRedisStorage(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	path := "/integration/pool"

	// Create and use stream to establish connections
	storage.Create(ctx, StreamConfig{Path: path})
	stream, _ := storage.Get(ctx, path)
	for i := 0; i < 10; i++ {
		stream.Append(ctx, []byte("test"))
	}

	// Check pool stats
	stats := storage.PoolStats()
	if stats.TotalConns == 0 {
		t.Error("Expected some connections in pool")
	}
	t.Logf("Pool stats: Hits=%d, Misses=%d, Timeouts=%d, TotalConns=%d, IdleConns=%d",
		stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns)
}
