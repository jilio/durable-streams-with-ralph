package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jilio/durable-streams-with-ralph/server"
	"github.com/jilio/durable-streams-with-ralph/stream"
)

func TestClient_New(t *testing.T) {
	c := New("http://example.com/stream")

	if c.URL != "http://example.com/stream" {
		t.Errorf("URL = %q, want %q", c.URL, "http://example.com/stream")
	}
}

func TestClient_NewWithOptions(t *testing.T) {
	httpClient := &http.Client{}
	headers := map[string]string{"Authorization": "Bearer token"}

	c := New("http://example.com/stream",
		WithHTTPClient(httpClient),
		WithHeaders(headers),
		WithContentType("application/json"),
	)

	if c.HTTPClient != httpClient {
		t.Error("HTTPClient not set correctly")
	}
	if c.Headers["Authorization"] != "Bearer token" {
		t.Error("Headers not set correctly")
	}
	if c.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", c.ContentType, "application/json")
	}
}

func TestClient_Head(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Create stream first
	ctx := context.Background()
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	// Test HEAD
	c := New(ts.URL + "/test/stream")
	result, err := c.Head(ctx)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	if !result.Exists {
		t.Error("Exists = false, want true")
	}
	if result.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", result.ContentType, "application/json")
	}
	if result.Offset == "" {
		t.Error("Offset is empty")
	}
}

func TestClient_HeadNotFound(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/nonexistent")
	result, err := c.Head(ctx)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	if result.Exists {
		t.Error("Exists = true, want false")
	}
}

func TestClient_Create(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/test/stream")

	err := c.Create(ctx, CreateOptions{
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify stream exists
	exists, _ := storage.Exists(ctx, "/test/stream")
	if !exists {
		t.Error("Stream was not created")
	}

	// Verify content type was updated
	if c.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", c.ContentType, "application/json")
	}
}

func TestClient_CreateWithInitialData(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/test/stream")

	err := c.Create(ctx, CreateOptions{
		ContentType: "application/json",
		Body:        []byte(`{"event":"initial"}`),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Read back the data
	str, _ := storage.Get(ctx, "/test/stream")
	batch, _ := str.ReadFrom(ctx, stream.StartOffset)
	if batch.Len() != 1 {
		t.Errorf("Len() = %d, want 1", batch.Len())
	}
}

func TestClient_CreateDuplicate(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/test/stream")

	// Create first time
	c.Create(ctx, CreateOptions{ContentType: "application/json"})

	// Create second time with same config should succeed (idempotent)
	err := c.Create(ctx, CreateOptions{ContentType: "application/json"})
	if err != nil {
		t.Errorf("Create() with same config should be idempotent, got error = %v", err)
	}

	// Create with different config should fail
	err = c.Create(ctx, CreateOptions{ContentType: "text/plain"})
	if err != ErrStreamExists {
		t.Errorf("Create() with different config error = %v, want ErrStreamExists", err)
	}
}

func TestClient_Delete(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream first
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := New(ts.URL + "/test/stream")
	err := c.Delete(ctx)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify stream no longer exists
	exists, _ := storage.Exists(ctx, "/test/stream")
	if exists {
		t.Error("Stream still exists after delete")
	}
}

func TestClient_DeleteNotFound(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/nonexistent")

	err := c.Delete(ctx)
	if err != ErrStreamNotFound {
		t.Errorf("Delete() error = %v, want ErrStreamNotFound", err)
	}
}

func TestClient_Append(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream first
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})

	c := New(ts.URL + "/test/stream")
	offset, err := c.Append(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if offset == "" {
		t.Error("Offset is empty")
	}
}

func TestClient_AppendNotFound(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/nonexistent")

	_, err := c.Append(ctx, []byte("hello"))
	if err != ErrStreamNotFound {
		t.Errorf("Append() error = %v, want ErrStreamNotFound", err)
	}
}

func TestClient_AppendEmpty(t *testing.T) {
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

	c := New(ts.URL + "/test/stream")
	_, err := c.Append(ctx, []byte{})
	if err == nil {
		t.Error("Append() with empty data should fail")
	}
}

func TestClient_AppendJSON(t *testing.T) {
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

	c := New(ts.URL+"/test/stream", WithContentType("application/json"))
	offset, err := c.AppendJSON(ctx, map[string]string{"event": "test"})
	if err != nil {
		t.Fatalf("AppendJSON() error = %v", err)
	}

	if offset == "" {
		t.Error("Offset is empty")
	}
}

func TestClient_Read(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	c := New(ts.URL+"/test/stream", WithContentType("application/json"))

	// Create stream with data via client
	c.Create(ctx, CreateOptions{ContentType: "application/json"})
	c.AppendJSON(ctx, map[string]string{"event": "first"})
	c.AppendJSON(ctx, map[string]string{"event": "second"})

	result, err := c.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(result.Messages) != 2 {
		t.Errorf("len(Messages) = %d, want 2", len(result.Messages))
	}
	if result.Offset == "" {
		t.Error("Offset is empty")
	}
	if !result.UpToDate {
		t.Error("UpToDate = false, want true")
	}
}

func TestClient_ReadNotFound(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/nonexistent")

	_, err := c.Read(ctx, stream.StartOffset)
	if err != ErrStreamNotFound {
		t.Errorf("Read() error = %v, want ErrStreamNotFound", err)
	}
}

func TestClient_ReadEmpty(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create empty stream
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := New(ts.URL + "/test/stream")
	result, err := c.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(result.Messages) != 0 {
		t.Errorf("len(Messages) = %d, want 0", len(result.Messages))
	}
	if !result.UpToDate {
		t.Error("UpToDate = false, want true")
	}
}

func TestClient_ReadFromOffset(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream with data
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})
	str, _ := storage.Get(ctx, "/test/stream")
	offset1, _ := str.Append(ctx, []byte(`{"event":"first"}`))
	str.Append(ctx, []byte(`{"event":"second"}`))

	c := New(ts.URL + "/test/stream")

	// Read from offset1 - should only get second message
	result, err := c.Read(ctx, offset1)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(result.Messages) != 1 {
		t.Errorf("len(Messages) = %d, want 1", len(result.Messages))
	}
}

func TestClient_ReadBinary(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create binary stream with data
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/octet-stream",
	})
	str, _ := storage.Get(ctx, "/test/stream")
	str.Append(ctx, []byte("hello"))
	str.Append(ctx, []byte("world"))

	c := New(ts.URL + "/test/stream")
	result, err := c.Read(ctx, stream.StartOffset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Binary mode returns concatenated data as single message
	if len(result.Messages) != 1 {
		t.Errorf("len(Messages) = %d, want 1", len(result.Messages))
	}
	if string(result.Messages[0].Data) != "helloworld" {
		t.Errorf("Data = %q, want %q", result.Messages[0].Data, "helloworld")
	}
}

func TestClient_CustomHeaders(t *testing.T) {
	var receivedAuth string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Stream-Next-Offset", "0_0")
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(handler)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL+"/stream", WithHeaders(map[string]string{
		"Authorization": "Bearer secret-token",
	}))

	c.Head(ctx)

	if receivedAuth != "Bearer secret-token" {
		t.Errorf("Authorization = %q, want %q", receivedAuth, "Bearer secret-token")
	}
}

func TestClient_Subscribe(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", 100*time.Millisecond) // Short timeout for tests
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream with initial data
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})
	str, _ := storage.Get(ctx, "/test/stream")
	str.Append(ctx, []byte(`{"event":"initial"}`))

	c := New(ts.URL + "/test/stream")

	// Start subscription
	subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	sub := c.Subscribe(subCtx, SubscribeOptions{
		Offset: stream.StartOffset,
	})

	// Read initial message
	select {
	case result := <-sub.Messages:
		if len(result.Messages) != 1 {
			t.Errorf("len(Messages) = %d, want 1", len(result.Messages))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for initial message")
	}

	// Append new message while subscribed
	str.Append(ctx, []byte(`{"event":"new"}`))

	// Should receive new message
	select {
	case result := <-sub.Messages:
		if len(result.Messages) != 1 {
			t.Errorf("len(Messages) = %d, want 1", len(result.Messages))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for new message")
	}

	// Cancel subscription
	sub.Cancel()

	// Channel should be closed
	select {
	case _, ok := <-sub.Messages:
		if ok {
			t.Error("Expected channel to be closed after cancel")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for channel to close")
	}
}

func TestClient_SubscribeFromNow(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", 100*time.Millisecond)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream with initial data
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})
	str, _ := storage.Get(ctx, "/test/stream")
	str.Append(ctx, []byte(`{"event":"old"}`))

	c := New(ts.URL + "/test/stream")

	// Start subscription from "now" - should not get old message
	subCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	sub := c.Subscribe(subCtx, SubscribeOptions{
		Offset: stream.NowOffset,
	})

	// Append new message
	go func() {
		time.Sleep(50 * time.Millisecond)
		str.Append(ctx, []byte(`{"event":"new"}`))
	}()

	// Should only receive new message, not old
	select {
	case result := <-sub.Messages:
		if string(result.Messages[0].Data) == `{"event":"old"}` {
			t.Error("Should not receive old message when subscribing from now")
		}
	case <-time.After(500 * time.Millisecond):
		// Timeout is ok - might not receive in time
	}

	sub.Cancel()
}

func TestClient_SubscribeNotFound(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/nonexistent")

	sub := c.Subscribe(ctx, SubscribeOptions{})

	// Should receive error
	select {
	case <-sub.Messages:
	case <-time.After(1 * time.Second):
	}

	if sub.Err() != ErrStreamNotFound {
		t.Errorf("Err() = %v, want ErrStreamNotFound", sub.Err())
	}
}

func TestClient_SubscribeOffset(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", 100*time.Millisecond)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream with data
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})
	str, _ := storage.Get(ctx, "/test/stream")
	str.Append(ctx, []byte(`{"event":"test"}`))

	c := New(ts.URL + "/test/stream")

	subCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	sub := c.Subscribe(subCtx, SubscribeOptions{
		Offset: stream.StartOffset,
	})

	// Get message
	<-sub.Messages

	// Offset should be updated
	offset := sub.Offset()
	if offset == "" || offset == stream.StartOffset {
		t.Error("Offset should be updated after receiving message")
	}

	sub.Cancel()
}

func TestClient_CreateWithTTL(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/test/stream")

	err := c.Create(ctx, CreateOptions{
		ContentType: "application/json",
		TTLSeconds:  3600,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	exists, _ := storage.Exists(ctx, "/test/stream")
	if !exists {
		t.Error("Stream was not created")
	}
}

func TestClient_CreateWithExpiresAt(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()
	c := New(ts.URL + "/test/stream")

	err := c.Create(ctx, CreateOptions{
		ContentType: "application/json",
		ExpiresAt:   "2030-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	exists, _ := storage.Exists(ctx, "/test/stream")
	if !exists {
		t.Error("Stream was not created")
	}
}

func TestClient_ReadWithOffset(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// Create stream with data
	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})
	str, _ := storage.Get(ctx, "/test/stream")
	str.Append(ctx, []byte(`{"event":"test"}`))

	c := New(ts.URL + "/test/stream")

	// Read with empty offset should work
	result, err := c.Read(ctx, "")
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(result.Messages) != 1 {
		t.Errorf("len(Messages) = %d, want 1", len(result.Messages))
	}
}

func TestClient_HeadCacheControl(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := server.New(storage)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	storage.Create(ctx, stream.StreamConfig{
		Path:        "/test/stream",
		ContentType: "application/json",
	})

	c := New(ts.URL + "/test/stream")
	result, err := c.Head(ctx)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	// HEAD should have Cache-Control: no-store
	if result.CacheControl != "no-store" {
		t.Errorf("CacheControl = %q, want %q", result.CacheControl, "no-store")
	}
}

func TestClient_WithCustomHTTPClient(t *testing.T) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	c := New("http://example.com/stream", WithHTTPClient(httpClient))

	if c.HTTPClient != httpClient {
		t.Error("HTTPClient should be set")
	}
}

func TestClient_DefaultHTTPClient(t *testing.T) {
	c := New("http://example.com/stream")

	// httpClient() should return default client
	client := c.httpClient()
	if client != http.DefaultClient {
		t.Error("httpClient() should return http.DefaultClient when HTTPClient is nil")
	}
}
