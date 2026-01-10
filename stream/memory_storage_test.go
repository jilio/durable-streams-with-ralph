package stream

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryStorage_Create(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	}

	err := storage.Create(ctx, config)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify stream exists
	exists, err := storage.Exists(ctx, "/test/stream")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false after Create()")
	}
}

func TestMemoryStorage_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	}

	storage.Create(ctx, config)
	err := storage.Create(ctx, config)

	if err != ErrStreamExists {
		t.Errorf("Create() duplicate error = %v, want ErrStreamExists", err)
	}
}

func TestMemoryStorage_CreateDefaultContentType(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path: "/test/stream",
		// No ContentType specified
	}

	storage.Create(ctx, config)

	meta, _ := storage.Head(ctx, "/test/stream")
	if meta.ContentType != DefaultContentType {
		t.Errorf("ContentType = %q, want %q", meta.ContentType, DefaultContentType)
	}
}

func TestMemoryStorage_Get(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	}
	storage.Create(ctx, config)

	stream, err := storage.Get(ctx, "/test/stream")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stream.Path() != "/test/stream" {
		t.Errorf("Path() = %q, want %q", stream.Path(), "/test/stream")
	}
	if stream.ContentType() != "application/json" {
		t.Errorf("ContentType() = %q, want %q", stream.ContentType(), "application/json")
	}
}

func TestMemoryStorage_GetNotFound(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	_, err := storage.Get(ctx, "/nonexistent")
	if err != ErrStreamNotFound {
		t.Errorf("Get() error = %v, want ErrStreamNotFound", err)
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	}
	storage.Create(ctx, config)

	err := storage.Delete(ctx, "/test/stream")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify stream no longer exists
	exists, _ := storage.Exists(ctx, "/test/stream")
	if exists {
		t.Error("Exists() = true after Delete()")
	}
}

func TestMemoryStorage_DeleteNotFound(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	err := storage.Delete(ctx, "/nonexistent")
	if err != ErrStreamNotFound {
		t.Errorf("Delete() error = %v, want ErrStreamNotFound", err)
	}
}

func TestMemoryStorage_Head(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	}
	storage.Create(ctx, config)

	meta, err := storage.Head(ctx, "/test/stream")
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if meta.Path != "/test/stream" {
		t.Errorf("Path = %q, want %q", meta.Path, "/test/stream")
	}
	if meta.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", meta.ContentType, "application/json")
	}
	if meta.NextOffset != InitialOffset {
		t.Errorf("NextOffset = %q, want %q", meta.NextOffset, InitialOffset)
	}
}

func TestMemoryStorage_HeadNotFound(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	_, err := storage.Head(ctx, "/nonexistent")
	if err != ErrStreamNotFound {
		t.Errorf("Head() error = %v, want ErrStreamNotFound", err)
	}
}

func TestMemoryStorage_AppendAndRead(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")

	// Append some data
	offset1, err := stream.Append(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	offset2, err := stream.Append(ctx, []byte("world"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read from start
	batch, err := stream.ReadFrom(ctx, StartOffset)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Errorf("Len() = %d, want 2", batch.Len())
	}

	// Verify message data
	if string(batch.Messages[0].Data) != "hello" {
		t.Errorf("Messages[0].Data = %q, want %q", batch.Messages[0].Data, "hello")
	}
	if string(batch.Messages[1].Data) != "world" {
		t.Errorf("Messages[1].Data = %q, want %q", batch.Messages[1].Data, "world")
	}

	// Read from offset1
	batch, _ = stream.ReadFrom(ctx, offset1)
	if batch.Len() != 1 {
		t.Errorf("ReadFrom(offset1) Len() = %d, want 1", batch.Len())
	}

	// Read from offset2 (should be empty)
	batch, _ = stream.ReadFrom(ctx, offset2)
	if batch.Len() != 0 {
		t.Errorf("ReadFrom(offset2) Len() = %d, want 0", batch.Len())
	}
}

func TestMemoryStorage_CurrentOffset(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")

	// Initial offset
	if stream.CurrentOffset() != InitialOffset {
		t.Errorf("CurrentOffset() = %q, want %q", stream.CurrentOffset(), InitialOffset)
	}

	// After append
	offset, _ := stream.Append(ctx, []byte("hello"))
	if stream.CurrentOffset() != offset {
		t.Errorf("CurrentOffset() = %q, want %q", stream.CurrentOffset(), offset)
	}
}

func TestMemoryStorage_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")

	// Concurrent appends
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stream.Append(ctx, []byte("msg"))
		}(i)
	}
	wg.Wait()

	// Verify all messages were appended
	batch, _ := stream.ReadFrom(ctx, StartOffset)
	if batch.Len() != 100 {
		t.Errorf("Len() = %d after concurrent appends, want 100", batch.Len())
	}
}

func TestMemoryStorage_Interface(t *testing.T) {
	// Verify MemoryStorage implements StreamStorage
	var _ StreamStorage = (*MemoryStorage)(nil)
}

func TestMemoryStream_Interface(t *testing.T) {
	// Verify memoryStream implements Stream
	var _ Stream = (*memoryStream)(nil)
}

func TestMemoryStorage_HeadAfterAppend(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")
	stream.Append(ctx, []byte("hello"))

	meta, _ := storage.Head(ctx, "/test/stream")
	if meta.NextOffset == InitialOffset {
		t.Error("NextOffset should be updated after append")
	}
}

func TestMemoryStorage_CreatedAt(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	before := time.Now().UnixMilli()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	}
	storage.Create(ctx, config)

	after := time.Now().UnixMilli()

	meta, _ := storage.Head(ctx, "/test/stream")
	if meta.CreatedAt < before || meta.CreatedAt > after {
		t.Errorf("CreatedAt = %d, want between %d and %d", meta.CreatedAt, before, after)
	}
}

func TestMemoryStorage_WaitForMessagesTimeout(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")

	// Wait with a short timeout - should time out since no messages
	result := stream.WaitForMessages(ctx, StartOffset, 50*time.Millisecond)
	if !result.TimedOut {
		t.Error("WaitForMessages should time out on empty stream")
	}
	if len(result.Messages) != 0 {
		t.Errorf("Messages = %d, want 0", len(result.Messages))
	}
}

func TestMemoryStorage_WaitForMessagesWithData(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")

	// Append data before waiting
	stream.Append(ctx, []byte("hello"))

	// Wait should return immediately with the message
	result := stream.WaitForMessages(ctx, StartOffset, 1*time.Second)
	if result.TimedOut {
		t.Error("WaitForMessages should not time out with existing messages")
	}
	if len(result.Messages) != 1 {
		t.Errorf("Messages = %d, want 1", len(result.Messages))
	}
}

func TestMemoryStorage_WaitForMessagesConcurrent(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()

	config := StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	}
	storage.Create(ctx, config)

	stream, _ := storage.Get(ctx, "/test/stream")
	currentOffset := stream.CurrentOffset()

	// Start waiting in goroutine
	done := make(chan WaitResult)
	go func() {
		done <- stream.WaitForMessages(ctx, currentOffset, 1*time.Second)
	}()

	// Give the wait time to start polling
	time.Sleep(20 * time.Millisecond)

	// Append data
	stream.Append(ctx, []byte("hello"))

	// Should receive the message
	select {
	case result := <-done:
		if result.TimedOut {
			t.Error("WaitForMessages should not time out")
		}
		if len(result.Messages) != 1 {
			t.Errorf("Messages = %d, want 1", len(result.Messages))
		}
	case <-time.After(2 * time.Second):
		t.Error("WaitForMessages did not return in time")
	}
}
