package stream

import (
	"context"
	"testing"
	"time"
)

// mockStream is a test implementation of the Stream interface
type mockStream struct {
	messages      []Message
	currentOffset Offset
	contentType   string
	path          string
}

func newMockStream(path, contentType string) *mockStream {
	return &mockStream{
		path:          path,
		contentType:   contentType,
		currentOffset: Offset("0_0"),
		messages:      make([]Message, 0),
	}
}

func (m *mockStream) Path() string {
	return m.path
}

func (m *mockStream) ContentType() string {
	return m.contentType
}

func (m *mockStream) CurrentOffset() Offset {
	return m.currentOffset
}

func (m *mockStream) Append(ctx context.Context, data []byte) (Offset, error) {
	offset := Offset("1_" + string(rune('0'+len(m.messages))))
	msg := NewMessage(data, offset, time.Now())
	m.messages = append(m.messages, msg)
	m.currentOffset = offset
	return offset, nil
}

func (m *mockStream) AppendWithSeq(ctx context.Context, data []byte, seq string) (Offset, error) {
	// Simple mock implementation - just append without seq checking
	return m.Append(ctx, data)
}

func (m *mockStream) AppendWithProducer(ctx context.Context, data []byte, producerId string, epoch, seq int64) (Offset, *ProducerResult) {
	// Simple mock implementation - just append
	offset, _ := m.Append(ctx, data)
	return offset, &ProducerResult{Status: ProducerStatusAccepted}
}

func (m *mockStream) ReadFrom(ctx context.Context, offset Offset) (Batch, error) {
	var msgs []Message
	for _, msg := range m.messages {
		if offset.IsStart() || msg.Offset.Compare(offset) > 0 {
			msgs = append(msgs, msg)
		}
	}
	return NewBatch(msgs, m.currentOffset), nil
}

func (m *mockStream) WaitForMessages(ctx context.Context, offset Offset, timeout time.Duration) WaitResult {
	// Simple mock implementation - just check for existing messages
	var msgs []Message
	for _, msg := range m.messages {
		if offset.IsStart() || msg.Offset.Compare(offset) > 0 {
			msgs = append(msgs, msg)
		}
	}
	if len(msgs) > 0 {
		return WaitResult{Messages: msgs, TimedOut: false}
	}
	return WaitResult{TimedOut: true}
}

// TestStreamInterface verifies that our mock implements Stream
func TestStreamInterface(t *testing.T) {
	var _ Stream = (*mockStream)(nil)
}

func TestStream_Path(t *testing.T) {
	s := newMockStream("/test/stream", "application/json")
	if got := s.Path(); got != "/test/stream" {
		t.Errorf("Path() = %q, want %q", got, "/test/stream")
	}
}

func TestStream_ContentType(t *testing.T) {
	s := newMockStream("/test/stream", "application/json")
	if got := s.ContentType(); got != "application/json" {
		t.Errorf("ContentType() = %q, want %q", got, "application/json")
	}
}

func TestStream_CurrentOffset(t *testing.T) {
	s := newMockStream("/test/stream", "application/json")
	if got := s.CurrentOffset(); got != Offset("0_0") {
		t.Errorf("CurrentOffset() = %q, want %q", got, "0_0")
	}
}

func TestStream_Append(t *testing.T) {
	ctx := context.Background()
	s := newMockStream("/test/stream", "application/json")

	offset, err := s.Append(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if offset == "" {
		t.Error("Append() returned empty offset")
	}
	if s.CurrentOffset() != offset {
		t.Errorf("CurrentOffset() = %q after append, want %q", s.CurrentOffset(), offset)
	}
}

func TestStream_ReadFrom(t *testing.T) {
	ctx := context.Background()
	s := newMockStream("/test/stream", "application/json")

	// Append some messages
	s.Append(ctx, []byte("msg1"))
	s.Append(ctx, []byte("msg2"))
	s.Append(ctx, []byte("msg3"))

	// Read from start
	batch, err := s.ReadFrom(ctx, StartOffset)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if batch.Len() != 3 {
		t.Errorf("ReadFrom(StartOffset) returned %d messages, want 3", batch.Len())
	}
}

func TestStream_ReadFromOffset(t *testing.T) {
	ctx := context.Background()
	s := newMockStream("/test/stream", "application/json")

	// Append some messages
	offset1, _ := s.Append(ctx, []byte("msg1"))
	s.Append(ctx, []byte("msg2"))
	s.Append(ctx, []byte("msg3"))

	// Read from after first message
	batch, err := s.ReadFrom(ctx, offset1)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Errorf("ReadFrom(offset1) returned %d messages, want 2", batch.Len())
	}
}

func TestStream_ReadFromEmpty(t *testing.T) {
	ctx := context.Background()
	s := newMockStream("/test/stream", "application/json")

	// Read from empty stream
	batch, err := s.ReadFrom(ctx, StartOffset)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if !batch.IsEmpty() {
		t.Errorf("ReadFrom() on empty stream returned %d messages, want 0", batch.Len())
	}
}

func TestStreamConfig_Normalize(t *testing.T) {
	tests := []struct {
		name            string
		contentType     string
		wantContentType string
	}{
		{"empty content type gets default", "", DefaultContentType},
		{"custom content type preserved", "application/json", "application/json"},
		{"text content type preserved", "text/plain", "text/plain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &StreamConfig{
				Path:        "/test",
				ContentType: tt.contentType,
			}
			cfg.Normalize()
			if cfg.ContentType != tt.wantContentType {
				t.Errorf("ContentType = %q, want %q", cfg.ContentType, tt.wantContentType)
			}
		})
	}
}
