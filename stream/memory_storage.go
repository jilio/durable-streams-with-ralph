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
	lastSeq   string // Last accepted Stream-Seq value
	producers map[string]*ProducerState
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

// CreateResult indicates what happened during stream creation.
type CreateResult struct {
	// Created is true if the stream was newly created.
	Created bool
	// Matched is true if stream existed with matching config (idempotent success).
	Matched bool
}

// Create creates a new stream with the given configuration.
// For idempotent PUT: returns ErrStreamExists if stream exists with matching config (for 200 response).
// Returns ErrConfigMismatch if stream exists with different config (for 409).
// Returns nil if a new stream was created (for 201).
func (s *MemoryStorage) Create(ctx context.Context, config StreamConfig) error {
	config.Normalize()

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.streams[config.Path]; exists {
		// Check if stream has expired
		if s.isExpired(existing) {
			// Expired stream - delete it and allow recreation
			delete(s.streams, config.Path)
		} else {
			// Stream exists - check for idempotent match
			if config.ConfigMatches(&existing.config) {
				// Config matches - idempotent success (return ErrStreamExists for 200)
				return ErrStreamExists
			}
			// Config mismatch - conflict (409)
			return ErrConfigMismatch
		}
	}

	s.streams[config.Path] = &memoryStreamData{
		config:    config,
		messages:  make([]Message, 0),
		offsetGen: NewOffsetGenerator(),
		createdAt: time.Now().UnixMilli(),
		producers: make(map[string]*ProducerState),
	}

	return nil // New stream created
}

// CreateOrMatch creates a stream or returns match status for idempotent PUT.
// Returns (true, nil) for new stream, (false, nil) for matched config,
// (false, ErrConfigMismatch) for config mismatch.
func (s *MemoryStorage) CreateOrMatch(ctx context.Context, config StreamConfig) (created bool, err error) {
	config.Normalize()

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.streams[config.Path]; exists {
		// Check if stream has expired
		if s.isExpired(existing) {
			// Expired stream - delete it and allow recreation
			delete(s.streams, config.Path)
		} else {
			// Stream exists - check for idempotent match
			if config.ConfigMatches(&existing.config) {
				// Config matches - idempotent success (200)
				return false, nil
			}
			// Config mismatch - conflict (409)
			return false, ErrConfigMismatch
		}
	}

	s.streams[config.Path] = &memoryStreamData{
		config:    config,
		messages:  make([]Message, 0),
		offsetGen: NewOffsetGenerator(),
		createdAt: time.Now().UnixMilli(),
		producers: make(map[string]*ProducerState),
	}

	return true, nil
}

// isExpired checks if a stream has expired based on TTL or ExpiresAt.
func (s *MemoryStorage) isExpired(data *memoryStreamData) bool {
	now := time.Now()

	// Check TTL
	if data.config.TTLSeconds > 0 {
		createdTime := time.UnixMilli(data.createdAt)
		expiresAt := createdTime.Add(time.Duration(data.config.TTLSeconds) * time.Second)
		if now.After(expiresAt) {
			return true
		}
	}

	// Check ExpiresAt
	if data.config.ExpiresAt != "" {
		expiresAt, err := time.Parse(time.RFC3339, data.config.ExpiresAt)
		if err == nil && now.After(expiresAt) {
			return true
		}
	}

	return false
}

// Get returns a Stream handle for the given path.
func (s *MemoryStorage) Get(ctx context.Context, path string) (Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, exists := s.streams[path]
	if !exists {
		return nil, ErrStreamNotFound
	}

	// Check if stream has expired
	if s.isExpired(data) {
		delete(s.streams, path)
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
	s.mu.Lock()
	defer s.mu.Unlock()

	data, exists := s.streams[path]
	if !exists {
		return false, nil
	}

	// Check if stream has expired
	if s.isExpired(data) {
		delete(s.streams, path)
		return false, nil
	}

	return true, nil
}

// Head returns metadata about a stream without reading its content.
func (s *MemoryStorage) Head(ctx context.Context, path string) (*StreamMetadata, error) {
	s.mu.Lock()
	data, exists := s.streams[path]
	if !exists {
		s.mu.Unlock()
		return nil, ErrStreamNotFound
	}

	// Check if stream has expired
	if s.isExpired(data) {
		delete(s.streams, path)
		s.mu.Unlock()
		return nil, ErrStreamNotFound
	}
	s.mu.Unlock()

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

// AppendWithSeq appends data with sequence ordering enforcement.
// Uses lexicographic string comparison for sequence ordering.
func (m *memoryStream) AppendWithSeq(ctx context.Context, data []byte, seq string) (Offset, error) {
	m.data.mu.Lock()
	defer m.data.mu.Unlock()

	// Check sequence ordering (lexicographic comparison)
	if seq != "" && m.data.lastSeq != "" && seq <= m.data.lastSeq {
		return "", ErrSeqConflict
	}

	// Generate the next offset
	offset := m.data.offsetGen.Next(len(data))

	// Create the message
	msg := NewMessage(data, offset, time.Now())
	m.data.messages = append(m.data.messages, msg)

	// Update lastSeq on successful append
	if seq != "" {
		m.data.lastSeq = seq
	}

	return offset, nil
}

// AppendWithProducer appends data with idempotent producer support.
// Returns the offset and producer validation result.
func (m *memoryStream) AppendWithProducer(ctx context.Context, data []byte, producerId string, epoch, seq int64) (Offset, *ProducerResult) {
	m.data.mu.Lock()
	defer m.data.mu.Unlock()

	now := time.Now().UnixMilli()

	// Initialize producers map if needed
	if m.data.producers == nil {
		m.data.producers = make(map[string]*ProducerState)
	}

	state := m.data.producers[producerId]

	// New producer - accept if seq is 0
	if state == nil {
		if seq != 0 {
			return "", &ProducerResult{
				Status:      ProducerStatusSequenceGap,
				ExpectedSeq: 0,
				ReceivedSeq: seq,
			}
		}
		// Accept new producer
		offset := m.data.offsetGen.Next(len(data))
		msg := NewMessage(data, offset, time.Now())
		m.data.messages = append(m.data.messages, msg)
		m.data.producers[producerId] = &ProducerState{
			Epoch:       epoch,
			LastSeq:     0,
			LastUpdated: now,
		}
		return offset, &ProducerResult{Status: ProducerStatusAccepted}
	}

	// Epoch validation (client-declared, server-validated)
	if epoch < state.Epoch {
		return "", &ProducerResult{
			Status:       ProducerStatusStaleEpoch,
			CurrentEpoch: state.Epoch,
		}
	}

	if epoch > state.Epoch {
		// New epoch must start at seq=0
		if seq != 0 {
			return "", &ProducerResult{Status: ProducerStatusInvalidEpochSeq}
		}
		// Accept new epoch
		offset := m.data.offsetGen.Next(len(data))
		msg := NewMessage(data, offset, time.Now())
		m.data.messages = append(m.data.messages, msg)
		m.data.producers[producerId] = &ProducerState{
			Epoch:       epoch,
			LastSeq:     0,
			LastUpdated: now,
		}
		return offset, &ProducerResult{Status: ProducerStatusAccepted}
	}

	// Same epoch: sequence validation
	if seq <= state.LastSeq {
		return "", &ProducerResult{
			Status:      ProducerStatusDuplicate,
			IsDuplicate: true,
			LastSeq:     state.LastSeq,
		}
	}

	if seq == state.LastSeq+1 {
		// Accept next sequence
		offset := m.data.offsetGen.Next(len(data))
		msg := NewMessage(data, offset, time.Now())
		m.data.messages = append(m.data.messages, msg)
		state.LastSeq = seq
		state.LastUpdated = now
		return offset, &ProducerResult{Status: ProducerStatusAccepted}
	}

	// Sequence gap
	return "", &ProducerResult{
		Status:      ProducerStatusSequenceGap,
		ExpectedSeq: state.LastSeq + 1,
		ReceivedSeq: seq,
	}
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
