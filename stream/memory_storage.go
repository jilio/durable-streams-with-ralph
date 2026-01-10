package stream

import (
	"context"
	"sync"
	"time"
)

// MemoryStorage is an in-memory implementation of StreamStorage.
// It is safe for concurrent access.
type MemoryStorage struct {
	mu      sync.RWMutex
	streams map[string]*memoryStreamData
}

// memoryStreamData holds the internal state for a stream.
type memoryStreamData struct {
	config    StreamConfig
	messages  []Message
	offsetGen *OffsetGenerator
	createdAt int64
	mu        sync.RWMutex
}

// memoryStream is a handle to an in-memory stream.
type memoryStream struct {
	data *memoryStreamData
}

// NewMemoryStorage creates a new in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		streams: make(map[string]*memoryStreamData),
	}
}

// Create creates a new stream with the given configuration.
func (s *MemoryStorage) Create(ctx context.Context, config StreamConfig) error {
	config.Normalize()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.streams[config.Path]; exists {
		return ErrStreamExists
	}

	s.streams[config.Path] = &memoryStreamData{
		config:    config,
		messages:  make([]Message, 0),
		offsetGen: NewOffsetGenerator(),
		createdAt: time.Now().UnixMilli(),
	}

	return nil
}

// Get returns a Stream handle for the given path.
func (s *MemoryStorage) Get(ctx context.Context, path string) (Stream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, exists := s.streams[path]
	if !exists {
		return nil, ErrStreamNotFound
	}

	return &memoryStream{data: data}, nil
}

// Delete removes a stream and all its data.
func (s *MemoryStorage) Delete(ctx context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.streams[path]; !exists {
		return ErrStreamNotFound
	}

	delete(s.streams, path)
	return nil
}

// Exists checks if a stream exists at the given path.
func (s *MemoryStorage) Exists(ctx context.Context, path string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.streams[path]
	return exists, nil
}

// Head returns metadata about a stream without reading its content.
func (s *MemoryStorage) Head(ctx context.Context, path string) (*StreamMetadata, error) {
	s.mu.RLock()
	data, exists := s.streams[path]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrStreamNotFound
	}

	data.mu.RLock()
	defer data.mu.RUnlock()

	return &StreamMetadata{
		Path:        data.config.Path,
		ContentType: data.config.ContentType,
		NextOffset:  data.offsetGen.Current(),
		TTLSeconds:  data.config.TTLSeconds,
		ExpiresAt:   data.config.ExpiresAt,
		CreatedAt:   data.createdAt,
	}, nil
}

// Path returns the stream's URL path.
func (m *memoryStream) Path() string {
	return m.data.config.Path
}

// ContentType returns the MIME content type of the stream.
func (m *memoryStream) ContentType() string {
	return m.data.config.ContentType
}

// CurrentOffset returns the current tail offset.
func (m *memoryStream) CurrentOffset() Offset {
	m.data.mu.RLock()
	defer m.data.mu.RUnlock()
	return m.data.offsetGen.Current()
}

// Append writes data to the stream and returns the assigned offset.
func (m *memoryStream) Append(ctx context.Context, data []byte) (Offset, error) {
	m.data.mu.Lock()
	defer m.data.mu.Unlock()

	// Generate the next offset
	offset := m.data.offsetGen.Next(len(data))

	// Create the message
	msg := NewMessage(data, offset, time.Now())
	m.data.messages = append(m.data.messages, msg)

	return offset, nil
}

// ReadFrom reads messages starting after the given offset.
func (m *memoryStream) ReadFrom(ctx context.Context, offset Offset) (Batch, error) {
	m.data.mu.RLock()
	defer m.data.mu.RUnlock()

	var msgs []Message

	for _, msg := range m.data.messages {
		// Include messages with offset > given offset
		// StartOffset (-1) means include all messages
		if offset.IsStart() || msg.Offset.Compare(offset) > 0 {
			msgs = append(msgs, msg)
		}
	}

	return NewBatch(msgs, m.data.offsetGen.Current()), nil
}

// WaitForMessages blocks until new messages are available or timeout expires.
func (m *memoryStream) WaitForMessages(ctx context.Context, offset Offset, timeout time.Duration) WaitResult {
	deadline := time.Now().Add(timeout)
	pollInterval := 10 * time.Millisecond

	for {
		// Check for new messages
		m.data.mu.RLock()
		var msgs []Message
		for _, msg := range m.data.messages {
			if offset.IsStart() || msg.Offset.Compare(offset) > 0 {
				msgs = append(msgs, msg)
			}
		}
		m.data.mu.RUnlock()

		if len(msgs) > 0 {
			return WaitResult{Messages: msgs, TimedOut: false}
		}

		// Check timeout
		if time.Now().After(deadline) {
			return WaitResult{TimedOut: true}
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return WaitResult{TimedOut: true}
		case <-time.After(pollInterval):
			// Continue polling
		}
	}
}

// Ensure interfaces are implemented
var (
	_ StreamStorage = (*MemoryStorage)(nil)
	_ Stream        = (*memoryStream)(nil)
)
