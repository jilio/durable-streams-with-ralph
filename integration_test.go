package durablestreams

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jilio/durable-streams-with-ralph/client"
	"github.com/jilio/durable-streams-with-ralph/server"
	"github.com/jilio/durable-streams-with-ralph/stream"
)

// Integration tests for client-server interaction

func TestIntegration_CreateReadDelete(t *testing.T) {
	// Setup server
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL + "/test/stream")

	// Create stream
	err := c.Create(ctx, client.CreateOptions{
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify with Head
	result, err := c.Head(ctx)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if !result.Exists {
		t.Error("Stream should exist after creation")
	}

	// Append data
	_, err = c.AppendJSON(ctx, map[string]string{"event": "test"})
	if err != nil {
		t.Fatalf("AppendJSON() error = %v", err)
	}

	// Read data
	readResult, err := c.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(readResult.Messages) != 1 {
		t.Errorf("len(Messages) = %d, want 1", len(readResult.Messages))
	}

	// Delete stream
	err = c.Delete(ctx)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify deleted
	result, _ = c.Head(ctx)
	if result.Exists {
		t.Error("Stream should not exist after deletion")
	}
}

func TestIntegration_SubscribeToLiveUpdates(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", 100*time.Millisecond)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := client.New(ts.URL + "/test/stream")

	// Subscribe from start
	subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	sub := c.Subscribe(subCtx, client.SubscribeOptions{
		Offset: stream.StartOffset,
	})

	// Producer goroutine
	go func() {
		time.Sleep(50 * time.Millisecond)
		str, _ := storage.Get(ctx, "/test/stream")
		str.Append(ctx, []byte(`{"event":"msg1"}`))
		time.Sleep(50 * time.Millisecond)
		str.Append(ctx, []byte(`{"event":"msg2"}`))
	}()

	// Collect messages
	received := 0
	for result := range sub.Messages {
		received += len(result.Messages)
		if received >= 2 {
			break
		}
	}

	sub.Cancel()

	if received < 2 {
		t.Errorf("Received = %d messages, want >= 2", received)
	}
}

func TestIntegration_IdempotentProducerBatching(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := client.New(ts.URL+"/test/stream", client.WithContentType("application/json"))
	p := client.NewIdempotentProducer(c, "test-producer",
		client.WithLingerMs(10),
		client.WithMaxBatchBytes(1000),
	)
	defer p.Close()

	// Append multiple messages
	for i := 0; i < 10; i++ {
		err := p.AppendJSON(map[string]int{"index": i})
		if err != nil {
			t.Fatalf("AppendJSON() error = %v", err)
		}
	}

	// Flush all
	err := p.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Read all messages
	readC := client.New(ts.URL + "/test/stream")
	result, err := readC.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Messages are batched - should have at least 1 message containing the batch
	if len(result.Messages) < 1 {
		t.Errorf("len(Messages) = %d, want >= 1", len(result.Messages))
	}
}

func TestIntegration_ConcurrentProducersAndConsumers(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", 100*time.Millisecond)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	var wg sync.WaitGroup
	numProducers := 3

	// Start producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			c := client.New(ts.URL+"/test/stream", client.WithContentType("application/json"))
			p := client.NewIdempotentProducer(c, "producer-"+string(rune('A'+producerID)),
				client.WithLingerMs(5),
			)
			defer p.Close()

			for j := 0; j < 5; j++ {
				p.AppendJSON(map[string]interface{}{
					"producer": producerID,
					"msg":      j,
				})
			}
			p.Flush()
		}(i)
	}

	wg.Wait()

	// Verify messages were written (batches from each producer)
	c := client.New(ts.URL + "/test/stream")
	result, err := c.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Each producer sends batched messages - expect at least numProducers batches
	if len(result.Messages) < numProducers {
		t.Errorf("len(Messages) = %d, want >= %d", len(result.Messages), numProducers)
	}
}

func TestIntegration_OffsetResumption(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	c := client.New(ts.URL+"/test/stream", client.WithContentType("application/json"))

	// Create stream
	err := c.Create(ctx, client.CreateOptions{ContentType: "application/json"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Append 5 messages via client, track middle offset
	var middleOffset stream.Offset
	for i := 0; i < 5; i++ {
		offset, err := c.AppendJSON(ctx, map[string]int{"index": i})
		if err != nil {
			t.Fatalf("AppendJSON() error = %v", err)
		}
		if i == 2 {
			middleOffset = offset
		}
	}

	// Read from middle offset
	result, err := c.Read(ctx, middleOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should get messages after middle (index 3 and 4)
	if len(result.Messages) != 2 {
		t.Errorf("len(Messages) = %d, want 2 (messages after middle offset)", len(result.Messages))
	}
}

func TestIntegration_BinaryStream(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create binary stream
	c := client.New(ts.URL + "/test/binary")
	err := c.Create(ctx, client.CreateOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Append binary data
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	_, err = c.Append(ctx, binaryData)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read back
	result, err := c.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(result.Messages) != 1 {
		t.Errorf("len(Messages) = %d, want 1", len(result.Messages))
	}

	// Binary data should match
	if string(result.Messages[0].Data) != string(binaryData) {
		t.Errorf("Data mismatch: got %v, want %v", result.Messages[0].Data, binaryData)
	}
}

func TestIntegration_LongPollTimeout(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", 50*time.Millisecond) // Very short timeout
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create empty stream
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := client.New(ts.URL + "/test/stream")

	// Subscribe to empty stream
	subCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	sub := c.Subscribe(subCtx, client.SubscribeOptions{
		Offset: stream.StartOffset,
	})

	// Wait for timeout - should not receive any messages
	select {
	case result := <-sub.Messages:
		if len(result.Messages) > 0 {
			t.Error("Should not receive messages from empty stream")
		}
	case <-time.After(150 * time.Millisecond):
		// Expected - no messages
	}

	sub.Cancel()
}

func TestIntegration_MultipleStreams(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create multiple streams
	streams := []string{"/stream/a", "/stream/b", "/stream/c"}
	for _, path := range streams {
		c := client.New(ts.URL + path)
		err := c.Create(ctx, client.CreateOptions{
			ContentType: "application/json",
		})
		if err != nil {
			t.Fatalf("Create(%s) error = %v", path, err)
		}

		// Append data to each
		c.AppendJSON(ctx, map[string]string{"stream": path})
	}

	// Read from each stream
	for _, path := range streams {
		c := client.New(ts.URL + path)
		result, err := c.Read(ctx, stream.StartOffset)
		if err != nil {
			t.Fatalf("Read(%s) error = %v", path, err)
		}
		if len(result.Messages) != 1 {
			t.Errorf("Stream %s: len(Messages) = %d, want 1", path, len(result.Messages))
		}
	}
}
