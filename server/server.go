package server

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/jilio/durable-streams-with-ralph/stream"
)

// HTTP header names for the durable streams protocol.
const (
	HeaderContentType    = "Content-Type"
	HeaderStreamOffset   = "Stream-Next-Offset"
	HeaderStreamTTL      = "Stream-TTL"
	HeaderStreamExpires  = "Stream-Expires-At"
	HeaderStreamCursor   = "Stream-Cursor"
	HeaderStreamUpToDate = "Stream-Up-To-Date"
	HeaderStreamSeq      = "Stream-Seq"
	HeaderLocation       = "Location"
	HeaderAllow          = "Allow"
)

// Server is an HTTP server for the durable streams protocol.
type Server struct {
	storage stream.StreamStorage
	baseURL string
}

// New creates a new durable streams server with the given storage backend.
func New(storage stream.StreamStorage) *Server {
	return &Server{
		storage: storage,
		baseURL: "",
	}
}

// NewWithBaseURL creates a new server with a custom base URL for Location headers.
func NewWithBaseURL(storage stream.StreamStorage, baseURL string) *Server {
	return &Server{
		storage: storage,
		baseURL: baseURL,
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	switch r.Method {
	case http.MethodPut:
		s.handleCreate(w, r, path)
	case http.MethodPost:
		s.handleAppend(w, r, path)
	case http.MethodOptions:
		s.handleOptions(w, r)
	default:
		w.Header().Set(HeaderAllow, "GET, POST, PUT, DELETE, HEAD, OPTIONS")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCreate handles PUT requests to create a new stream.
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	// Get and validate content type
	contentType := r.Header.Get(HeaderContentType)
	if contentType == "" || !isValidContentType(contentType) {
		contentType = stream.DefaultContentType
	}

	// Get TTL header
	ttlHeader := r.Header.Get(HeaderStreamTTL)
	expiresHeader := r.Header.Get(HeaderStreamExpires)

	// Cannot specify both TTL and Expires-At
	if ttlHeader != "" && expiresHeader != "" {
		http.Error(w, "Cannot specify both Stream-TTL and Stream-Expires-At", http.StatusBadRequest)
		return
	}

	// Validate TTL
	var ttlSeconds int
	if ttlHeader != "" {
		var err error
		ttlSeconds, err = parseTTL(ttlHeader)
		if err != nil {
			http.Error(w, "Invalid Stream-TTL value", http.StatusBadRequest)
			return
		}
	}

	// Validate Expires-At
	var expiresAt string
	if expiresHeader != "" {
		_, err := time.Parse(time.RFC3339, expiresHeader)
		if err != nil {
			http.Error(w, "Invalid Stream-Expires-At timestamp", http.StatusBadRequest)
			return
		}
		expiresAt = expiresHeader
	}

	// Read body if present (for initial data)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	// Create stream config
	config := stream.StreamConfig{
		Path:        path,
		ContentType: contentType,
		TTLSeconds:  ttlSeconds,
		ExpiresAt:   expiresAt,
	}

	// Create the stream
	err = s.storage.Create(ctx, config)
	if err == stream.ErrStreamExists {
		// Stream already exists - return 409 Conflict
		http.Error(w, "Stream already exists", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, "Failed to create stream", http.StatusInternalServerError)
		return
	}

	// If initial data was provided, append it
	var nextOffset stream.Offset = stream.InitialOffset
	if len(body) > 0 {
		str, err := s.storage.Get(ctx, path)
		if err != nil {
			http.Error(w, "Failed to get stream", http.StatusInternalServerError)
			return
		}
		nextOffset, err = str.Append(ctx, body)
		if err != nil {
			http.Error(w, "Failed to append initial data", http.StatusInternalServerError)
			return
		}
	} else {
		// Get the current offset
		meta, err := s.storage.Head(ctx, path)
		if err == nil {
			nextOffset = meta.NextOffset
		}
	}

	// Set response headers
	w.Header().Set(HeaderContentType, contentType)
	w.Header().Set(HeaderStreamOffset, string(nextOffset))
	w.Header().Set(HeaderLocation, s.baseURL+path)

	w.WriteHeader(http.StatusCreated)
}

// handleAppend handles POST requests to append data to a stream.
func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	// Content-Type is required for POST
	contentType := r.Header.Get(HeaderContentType)
	if contentType == "" {
		http.Error(w, "Content-Type header is required", http.StatusBadRequest)
		return
	}

	// Read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	// Empty body is not allowed for append
	if len(body) == 0 {
		http.Error(w, "Empty body", http.StatusBadRequest)
		return
	}

	// Get the stream
	str, ok := s.getStream(ctx, w, path)
	if !ok {
		return
	}

	// Append the data
	nextOffset, err := str.Append(ctx, body)
	if err != nil {
		http.Error(w, "Failed to append data", http.StatusInternalServerError)
		return
	}

	// Set response headers
	w.Header().Set(HeaderStreamOffset, string(nextOffset))
	w.WriteHeader(http.StatusOK)
}

// handleOptions handles OPTIONS requests for CORS preflight.
func (s *Server) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HeaderAllow, "GET, POST, PUT, DELETE, HEAD, OPTIONS")
	w.WriteHeader(http.StatusOK)
}

// isValidContentType checks if a content type string is valid.
func isValidContentType(ct string) bool {
	// Basic MIME type format: type/subtype
	pattern := regexp.MustCompile(`^[\w-]+/[\w-]+`)
	return pattern.MatchString(ct)
}

// parseTTL parses and validates a TTL header value.
// TTL must be a non-negative integer without leading zeros, decimals, etc.
func parseTTL(s string) (int, error) {
	// Must match: "0" or a number starting with 1-9
	pattern := regexp.MustCompile(`^(0|[1-9]\d*)$`)
	if !pattern.MatchString(s) {
		return 0, strconv.ErrSyntax
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, strconv.ErrRange
	}

	return n, nil
}

// Helper to get stream or return 404
func (s *Server) getStream(ctx context.Context, w http.ResponseWriter, path string) (stream.Stream, bool) {
	str, err := s.storage.Get(ctx, path)
	if err == stream.ErrStreamNotFound {
		http.Error(w, "Stream not found", http.StatusNotFound)
		return nil, false
	}
	if err != nil {
		http.Error(w, "Failed to get stream", http.StatusInternalServerError)
		return nil, false
	}
	return str, true
}
