package client

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jilio/durable-streams-with-ralph/server"
	"github.com/jilio/durable-streams-with-ralph/stream"
)

func TestIdempotentProducer_New(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer")

	if p.Epoch() != 0 {
		t.Errorf("Epoch() = %d, want 0", p.Epoch())
	}
	if p.Seq() != 0 {
		t.Errorf("Seq() = %d, want 0", p.Seq())
	}

	p.Close()
}

func TestIdempotentProducer_WithOptions(t *testing.T) {
	c := New("http://example.com/stream")
	var calledError int32

	p := NewIdempotentProducer(c, "test-producer",
		WithEpoch(5),
		WithMaxBatchBytes(512),
		WithLingerMs(10),
		WithMaxInFlight(3),
		WithOnError(func(err error) { atomic.AddInt32(&calledError, 1) }),
	)

	if p.Epoch() != 5 {
		t.Errorf("Epoch() = %d, want 5", p.Epoch())
	}

	p.Close()
}

func TestIdempotentProducer_Append(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Create stream
	storage.Create(t.Context(), stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})

	c := New(ts.URL + "/test/stream")
	p := NewIdempotentProducer(c, "test-producer", WithLingerMs(1))
	defer p.Close()

	// Append data
	err := p.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Flush to ensure sent
	err = p.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify data was written
	str, _ := storage.Get(t.Context(), "/test/stream")
	batch, _ := str.ReadFrom(t.Context(), stream.StartOffset)
	if batch.Len() != 1 {
		t.Errorf("Len() = %d, want 1", batch.Len())
	}
}

func TestIdempotentProducer_AppendJSON(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Create stream
	storage.Create(t.Context(), stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := New(ts.URL+"/test/stream", WithContentType("application/json"))
	p := NewIdempotentProducer(c, "test-producer", WithLingerMs(1))
	defer p.Close()

	// Append JSON
	err := p.AppendJSON(map[string]string{"event": "test"})
	if err != nil {
		t.Fatalf("AppendJSON() error = %v", err)
	}

	// Flush
	err = p.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify data
	str, _ := storage.Get(t.Context(), "/test/stream")
	batch, _ := str.ReadFrom(t.Context(), stream.StartOffset)
	if batch.Len() != 1 {
		t.Errorf("Len() = %d, want 1", batch.Len())
	}
}

func TestIdempotentProducer_Batching(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Create stream
	storage.Create(t.Context(), stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})

	c := New(ts.URL + "/test/stream")
	p := NewIdempotentProducer(c, "test-producer",
		WithLingerMs(50),      // 50ms linger
		WithMaxBatchBytes(100), // small batch size
	)
	defer p.Close()

	// Append multiple messages quickly - should batch
	for i := 0; i < 5; i++ {
		p.Append([]byte("msg"))
	}

	// Flush
	p.Flush()

	// Verify data was written
	str, _ := storage.Get(t.Context(), "/test/stream")
	batch, _ := str.ReadFrom(t.Context(), stream.StartOffset)
	// All 5 messages should be there (batched together)
	if batch.Len() < 1 {
		t.Errorf("Len() = %d, want >= 1", batch.Len())
	}
}

func TestIdempotentProducer_LingerTimer(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Create stream
	storage.Create(t.Context(), stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})

	c := New(ts.URL + "/test/stream")
	p := NewIdempotentProducer(c, "test-producer",
		WithLingerMs(20), // 20ms linger
	)
	defer p.Close()

	// Append one message
	p.Append([]byte("test"))

	// Wait for linger timer to fire
	time.Sleep(50 * time.Millisecond)

	// Verify data was written
	str, _ := storage.Get(t.Context(), "/test/stream")
	batch, _ := str.ReadFrom(t.Context(), stream.StartOffset)
	if batch.Len() != 1 {
		t.Errorf("Len() = %d, want 1", batch.Len())
	}
}

func TestIdempotentProducer_Restart(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer")
	defer p.Close()

	if p.Epoch() != 0 {
		t.Errorf("Initial Epoch() = %d, want 0", p.Epoch())
	}

	p.Restart()

	if p.Epoch() != 1 {
		t.Errorf("After Restart() Epoch() = %d, want 1", p.Epoch())
	}
	if p.Seq() != 0 {
		t.Errorf("After Restart() Seq() = %d, want 0", p.Seq())
	}
}

func TestIdempotentProducer_Close(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer")

	err := p.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Append after close should fail
	err = p.Append([]byte("test"))
	if err != ErrProducerClosed {
		t.Errorf("Append() after Close() error = %v, want ErrProducerClosed", err)
	}
}

func TestIdempotentProducer_PendingCount(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer",
		WithLingerMs(1000), // Long linger so batch doesn't auto-send
	)
	defer p.Close()

	if p.PendingCount() != 0 {
		t.Errorf("Initial PendingCount() = %d, want 0", p.PendingCount())
	}

	p.Append([]byte("test"))

	if p.PendingCount() != 1 {
		t.Errorf("After Append() PendingCount() = %d, want 1", p.PendingCount())
	}
}

func TestIdempotentProducer_SeqIncrement(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	storage.Create(t.Context(), stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})

	c := New(ts.URL + "/test/stream")
	p := NewIdempotentProducer(c, "test-producer", WithLingerMs(1))
	defer p.Close()

	initialSeq := p.Seq()

	p.Append([]byte("msg1"))
	p.Flush()

	// Seq should have incremented
	if p.Seq() <= initialSeq {
		t.Errorf("Seq() = %d, should be > %d after append", p.Seq(), initialSeq)
	}
}

func TestIdempotentProducer_NotFound(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := New(ts.URL + "/nonexistent")
	p := NewIdempotentProducer(c, "test-producer", WithLingerMs(1))
	defer p.Close()

	p.Append([]byte("test"))
	err := p.Flush()

	if err != ErrStreamNotFound {
		t.Errorf("Flush() error = %v, want ErrStreamNotFound", err)
	}
}

func TestIdempotentProducer_AutoClaim(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer", WithAutoClaim(true))
	defer p.Close()

	// Just test that option is set without error
	if p.Epoch() != 0 {
		t.Errorf("Epoch() = %d, want 0", p.Epoch())
	}
}

func TestIdempotentProducer_InFlightCount(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer",
		WithLingerMs(1000), // Long linger
		WithMaxInFlight(5),
	)
	defer p.Close()

	// Initially 0
	if p.InFlightCount() != 0 {
		t.Errorf("InFlightCount() = %d, want 0", p.InFlightCount())
	}
}

func TestIdempotentProducer_AppendJSONError(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer", WithLingerMs(1000))
	defer p.Close()

	// Marshal error for channels
	err := p.AppendJSON(make(chan int))
	if err == nil {
		t.Error("AppendJSON() with channel should fail")
	}
}

func TestIdempotentProducer_OnError(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	var errorCalled int32

	c := New(ts.URL + "/nonexistent")
	p := NewIdempotentProducer(c, "test-producer",
		WithLingerMs(1),
		WithOnError(func(err error) { atomic.AddInt32(&errorCalled, 1) }),
	)
	defer p.Close()

	p.Append([]byte("test"))
	p.Flush()

	// Error callback should have been called
	if atomic.LoadInt32(&errorCalled) == 0 {
		t.Error("OnError callback should have been called")
	}
}

func TestIdempotentProducer_CloseIdempotent(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer")

	// Close twice - should be idempotent
	err1 := p.Close()
	err2 := p.Close()

	if err1 != nil || err2 != nil {
		t.Errorf("Close() errors = %v, %v; want nil, nil", err1, err2)
	}
}

func TestIdempotentProducer_MaxBatchBytes(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	storage.Create(t.Context(), stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})

	c := New(ts.URL + "/test/stream")
	p := NewIdempotentProducer(c, "test-producer",
		WithMaxBatchBytes(10), // Small batch size
		WithLingerMs(5000),    // Long linger so size triggers
	)
	defer p.Close()

	// Append data that exceeds max batch bytes
	// This should trigger immediate batch send
	p.Append([]byte("1234567890123")) // 13 bytes > 10

	// Give time for batch to send
	time.Sleep(50 * time.Millisecond)

	// Verify data was written
	str, _ := storage.Get(t.Context(), "/test/stream")
	batch, _ := str.ReadFrom(t.Context(), stream.StartOffset)
	if batch.Len() < 1 {
		t.Errorf("Len() = %d, want >= 1", batch.Len())
	}
}

func TestIdempotentProducer_AppendJSONClosed(t *testing.T) {
	c := New("http://example.com/stream")
	p := NewIdempotentProducer(c, "test-producer")
	p.Close()

	err := p.AppendJSON(map[string]string{"test": "value"})
	if err != ErrProducerClosed {
		t.Errorf("AppendJSON() after close error = %v, want ErrProducerClosed", err)
	}
}
