package stream

import (
	"context"
	"testing"
	"time"
)

// mockStorage implements StreamStorage for testing
type mockStorage struct {
	streams map[string]*mockStorageStream
}

type mockStorageStream struct {
	config   StreamConfig
	messages []Message
	offset   Offset
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		streams: make(map[string]*mockStorageStream),
	}
}

func (s *mockStorage) Create(ctx context.Context, config StreamConfig) error {
	if _, exists := s.streams[config.Path]; exists {
		return ErrStreamExists
	}
	config.Normalize()
	s.streams[config.Path] = &mockStorageStream{
		config:   config,
		messages: make([]Message, 0),
		offset:   Offset("0_0"),
	}
	return nil
}

func (s *mockStorage) Get(ctx context.Context, path string) (Stream, error) {
	stream, exists := s.streams[path]
	if !exists {
		return nil, ErrStreamNotFound
	}
	return &mockStorageStreamWrapper{stream}, nil
}

func (s *mockStorage) Delete(ctx context.Context, path string) error {
	if _, exists := s.streams[path]; !exists {
		return ErrStreamNotFound
	}
	delete(s.streams, path)
	return nil
}

func (s *mockStorage) Exists(ctx context.Context, path string) (bool, error) {
	_, exists := s.streams[path]
	return exists, nil
}

func (s *mockStorage) Head(ctx context.Context, path string) (*StreamMetadata, error) {
	stream, exists := s.streams[path]
	if !exists {
		return nil, ErrStreamNotFound
	}
	return &StreamMetadata{
		Path:        stream.config.Path,
		ContentType: stream.config.ContentType,
		NextOffset:  stream.offset,
	}, nil
}

// mockStorageStreamWrapper wraps mockStorageStream to implement Stream interface
type mockStorageStreamWrapper struct {
	s *mockStorageStream
}

func (w *mockStorageStreamWrapper) Path() string {
	return w.s.config.Path
}

func (w *mockStorageStreamWrapper) ContentType() string {
	return w.s.config.ContentType
}

func (w *mockStorageStreamWrapper) CurrentOffset() Offset {
	return w.s.offset
}

func (w *mockStorageStreamWrapper) Append(ctx context.Context, data []byte) (Offset, error) {
	offset := Offset("1_" + string(rune('0'+len(w.s.messages))))
	msg := NewMessage(data, offset, time.Now())
	w.s.messages = append(w.s.messages, msg)
	w.s.offset = offset
	return offset, nil
}

func (w *mockStorageStreamWrapper) ReadFrom(ctx context.Context, offset Offset) (Batch, error) {
	var msgs []Message
	for _, msg := range w.s.messages {
		if offset.IsStart() || msg.Offset.Compare(offset) > 0 {
			msgs = append(msgs, msg)
		}
	}
	return NewBatch(msgs, w.s.offset), nil
}

// TestStreamStorageInterface verifies that mockStorage implements StreamStorage
func TestStreamStorageInterface(t *testing.T) {
	var _ StreamStorage = (*mockStorage)(nil)
}

func TestStreamStorage_Create(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

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

func TestStreamStorage_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

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

func TestStreamStorage_Get(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

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

func TestStreamStorage_GetNotFound(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

	_, err := storage.Get(ctx, "/nonexistent")
	if err != ErrStreamNotFound {
		t.Errorf("Get() error = %v, want ErrStreamNotFound", err)
	}
}

func TestStreamStorage_Delete(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

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

func TestStreamStorage_DeleteNotFound(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

	err := storage.Delete(ctx, "/nonexistent")
	if err != ErrStreamNotFound {
		t.Errorf("Delete() error = %v, want ErrStreamNotFound", err)
	}
}

func TestStreamStorage_Head(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

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
}

func TestStreamStorage_HeadNotFound(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()

	_, err := storage.Head(ctx, "/nonexistent")
	if err != ErrStreamNotFound {
		t.Errorf("Head() error = %v, want ErrStreamNotFound", err)
	}
}

func TestStreamMetadata(t *testing.T) {
	meta := StreamMetadata{
		Path:        "/test",
		ContentType: "application/json",
		NextOffset:  Offset("1_100"),
		TTLSeconds:  3600,
		ExpiresAt:   "2025-01-01T00:00:00Z",
	}

	if meta.Path != "/test" {
		t.Errorf("Path = %q, want %q", meta.Path, "/test")
	}
	if meta.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", meta.ContentType, "application/json")
	}
	if meta.NextOffset != Offset("1_100") {
		t.Errorf("NextOffset = %q, want %q", meta.NextOffset, "1_100")
	}
	if meta.TTLSeconds != 3600 {
		t.Errorf("TTLSeconds = %d, want %d", meta.TTLSeconds, 3600)
	}
	if meta.ExpiresAt != "2025-01-01T00:00:00Z" {
		t.Errorf("ExpiresAt = %q, want %q", meta.ExpiresAt, "2025-01-01T00:00:00Z")
	}
}
