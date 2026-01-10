package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jilio/durable-streams-with-ralph/stream"
)

func TestServer_PutCreateStream(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	// Check headers
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
	if offset := resp.Header.Get("Stream-Next-Offset"); offset == "" {
		t.Error("Stream-Next-Offset header missing")
	}
	if loc := resp.Header.Get("Location"); loc == "" {
		t.Error("Location header missing for 201 Created")
	}
}

func TestServer_PutCreateStreamDefaultContentType(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	// No Content-Type header

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/octet-stream")
	}
}

func TestServer_PutIdempotentCreate(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	// First create
	req1 := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	srv.ServeHTTP(w1, req1)

	if w1.Result().StatusCode != http.StatusCreated {
		t.Fatalf("First create StatusCode = %d, want %d", w1.Result().StatusCode, http.StatusCreated)
	}

	// Second create (idempotent) - should return 409 Conflict per our current impl
	// Note: Reference impl returns 200 for matching config, 409 for different config
	req2 := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, req2)

	// For now, we return 409 Conflict for existing stream
	if w2.Result().StatusCode != http.StatusConflict {
		t.Errorf("Second create StatusCode = %d, want %d", w2.Result().StatusCode, http.StatusConflict)
	}
}

func TestServer_PutWithInitialData(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	body := strings.NewReader(`{"event":"created"}`)
	req := httptest.NewRequest(http.MethodPut, "/test/stream", body)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	// Verify offset is non-initial (data was appended)
	offset := resp.Header.Get("Stream-Next-Offset")
	if offset == string(stream.InitialOffset) {
		t.Error("Stream-Next-Offset should be updated after initial data")
	}
}

func TestServer_PutWithTTL(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-TTL", "3600")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestServer_PutWithInvalidTTL(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	tests := []struct {
		name string
		ttl  string
	}{
		{"negative", "-1"},
		{"decimal", "3600.5"},
		{"leading_zero", "03600"},
		{"plus_sign", "+3600"},
		{"letters", "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/test/stream/"+tt.name, nil)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Stream-TTL", tt.ttl)

			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)

			resp := w.Result()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("StatusCode = %d, want %d for TTL %q", resp.StatusCode, http.StatusBadRequest, tt.ttl)
			}
		})
	}
}

func TestServer_PutWithExpiresAt(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-Expires-At", "2030-01-01T00:00:00Z")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestServer_PutWithInvalidExpiresAt(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-Expires-At", "invalid-timestamp")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestServer_PutWithBothTTLAndExpiresAt(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-TTL", "3600")
	req.Header.Set("Stream-Expires-At", "2030-01-01T00:00:00Z")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "both") {
		t.Errorf("Body = %q, want to contain 'both'", body)
	}
}

func TestServer_MethodNotAllowed(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPatch, "/test/stream", nil)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}

	allow := resp.Header.Get("Allow")
	if allow == "" {
		t.Error("Allow header missing")
	}
}

func TestServer_Options(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodOptions, "/test/stream", nil)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	allow := resp.Header.Get("Allow")
	if allow == "" {
		t.Error("Allow header missing")
	}
}

func TestServer_NewWithBaseURL(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := NewWithBaseURL(storage, "http://example.com")

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	loc := resp.Header.Get("Location")
	if loc != "http://example.com/test/stream" {
		t.Errorf("Location = %q, want %q", loc, "http://example.com/test/stream")
	}
}

func TestServer_PutZeroTTL(t *testing.T) {
	storage := stream.NewMemoryStorage()
	srv := New(storage)

	req := httptest.NewRequest(http.MethodPut, "/test/stream", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-TTL", "0")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}
