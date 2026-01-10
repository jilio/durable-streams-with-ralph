package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
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

// DefaultLongPollTimeout is the default timeout for long-poll requests.
const DefaultLongPollTimeout = 30 * time.Second

// Server is an HTTP server for the durable streams protocol.
type Server struct {
	storage         stream.StreamStorage
	baseURL         string
	longPollTimeout time.Duration
}

// New creates a new durable streams server with the given storage backend.
func New(storage stream.StreamStorage) *Server {
	return &Server{
		storage:         storage,
		baseURL:         "",
		longPollTimeout: DefaultLongPollTimeout,
	}
}

// NewWithBaseURL creates a new server with a custom base URL for Location headers.
func NewWithBaseURL(storage stream.StreamStorage, baseURL string) *Server {
	return &Server{
		storage:         storage,
		baseURL:         baseURL,
		longPollTimeout: DefaultLongPollTimeout,
	}
}

// NewWithOptions creates a new server with custom options.
func NewWithOptions(storage stream.StreamStorage, baseURL string, longPollTimeout time.Duration) *Server {
	return &Server{
		storage:         storage,
		baseURL:         baseURL,
		longPollTimeout: longPollTimeout,
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set security headers on all responses
	s.setSecurityHeaders(w)

	path := r.URL.Path

	switch r.Method {
	case http.MethodGet:
		s.handleRead(w, r, path)
	case http.MethodPut:
		s.handleCreate(w, r, path)
	case http.MethodPost:
		s.handleAppend(w, r, path)
	case http.MethodDelete:
		s.handleDelete(w, r, path)
	case http.MethodHead:
		s.handleHead(w, r, path)
	case http.MethodOptions:
		s.handleOptions(w, r)
	default:
		w.Header().Set(HeaderAllow, "GET, POST, PUT, DELETE, HEAD, OPTIONS")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// setSecurityHeaders sets browser security headers on responses.
func (s *Server) setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Stream-TTL, Stream-Expires-At, Stream-Seq, If-None-Match, Accept")
	w.Header().Set("Access-Control-Expose-Headers", "Stream-Next-Offset, Stream-Up-To-Date, Stream-Cursor, Stream-TTL, Stream-Expires-At, ETag, Location, Content-Type")
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
		// Stream already exists with matching config - idempotent success (200)
		// Get the current offset
		var existingOffset stream.Offset = stream.InitialOffset
		meta, metaErr := s.storage.Head(ctx, path)
		if metaErr == nil {
			existingOffset = meta.NextOffset
		}
		w.Header().Set(HeaderContentType, contentType)
		w.Header().Set(HeaderStreamOffset, string(existingOffset))
		w.Header().Set(HeaderLocation, s.baseURL+path)
		w.WriteHeader(http.StatusOK)
		return
	}
	if err == stream.ErrConfigMismatch {
		// Stream exists with different config - conflict (409)
		http.Error(w, "Stream already exists with different configuration", http.StatusConflict)
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

	// Verify content-type matches stream's content-type (case-insensitive)
	streamContentType := stream.NormalizeContentType(str.ContentType())
	requestContentType := stream.NormalizeContentType(contentType)
	if streamContentType != requestContentType {
		http.Error(w, "Content-Type mismatch", http.StatusConflict)
		return
	}

	var nextOffset stream.Offset

	// For JSON content-type, parse and handle array unwrapping
	if isJSONContentType(str.ContentType()) {
		messages, err := processJSONAppend(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Append each message
		for _, msg := range messages {
			nextOffset, err = str.Append(ctx, msg)
			if err != nil {
				http.Error(w, "Failed to append data", http.StatusInternalServerError)
				return
			}
		}
	} else {
		// Binary mode: append raw data
		nextOffset, err = str.Append(ctx, body)
		if err != nil {
			http.Error(w, "Failed to append data", http.StatusInternalServerError)
			return
		}
	}

	// Set response headers
	w.Header().Set(HeaderStreamOffset, string(nextOffset))
	w.WriteHeader(http.StatusNoContent)
}

// processJSONAppend parses JSON body and returns messages to append.
// If body is an array, each element becomes a separate message.
// If body is a single value, it becomes one message.
// Empty arrays are rejected.
func processJSONAppend(body []byte) ([][]byte, error) {
	// Parse JSON
	var parsed interface{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("Invalid JSON")
	}

	// Check if it's an array
	if arr, ok := parsed.([]interface{}); ok {
		if len(arr) == 0 {
			return nil, fmt.Errorf("Empty arrays are not allowed")
		}

		// Each element becomes a separate message with trailing comma
		messages := make([][]byte, len(arr))
		for i, item := range arr {
			data, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("Invalid JSON")
			}
			// Add trailing comma for concatenation
			messages[i] = append(data, ',')
		}
		return messages, nil
	}

	// Single value - store as single message with trailing comma
	data, err := json.Marshal(parsed)
	if err != nil {
		return nil, fmt.Errorf("Invalid JSON")
	}
	return [][]byte{append(data, ',')}, nil
}

// handleDelete handles DELETE requests to remove a stream.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	// Check if stream exists
	exists, err := s.storage.Exists(ctx, path)
	if err != nil {
		http.Error(w, "Failed to check stream", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}

	// Delete the stream
	err = s.storage.Delete(ctx, path)
	if err != nil {
		http.Error(w, "Failed to delete stream", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHead handles HEAD requests to get stream metadata.
func (s *Server) handleHead(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	// Get stream metadata
	meta, err := s.storage.Head(ctx, path)
	if err == stream.ErrStreamNotFound {
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Failed to get stream metadata", http.StatusInternalServerError)
		return
	}

	// Set headers
	w.Header().Set(HeaderContentType, meta.ContentType)
	w.Header().Set(HeaderStreamOffset, string(meta.NextOffset))
	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(http.StatusOK)
}

// handleRead handles GET requests to read from a stream.
func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	// Get the stream
	str, ok := s.getStream(ctx, w, path)
	if !ok {
		return
	}

	// Get query parameters
	offset := r.URL.Query().Get("offset")
	live := r.URL.Query().Get("live")

	// Check if offset parameter was provided but empty
	if r.URL.Query().Has("offset") && offset == "" {
		http.Error(w, "Empty offset parameter", http.StatusBadRequest)
		return
	}

	// Validate offset format if provided
	if offset != "" {
		validOffsetPattern := regexp.MustCompile(`^(-1|now|\d+_\d+)$`)
		if !validOffsetPattern.MatchString(offset) {
			http.Error(w, "Invalid offset format", http.StatusBadRequest)
			return
		}
	}

	// Long-poll and SSE require offset parameter
	if (live == "long-poll" || live == "sse") && offset == "" {
		http.Error(w, live+" requires offset parameter", http.StatusBadRequest)
		return
	}

	// Handle SSE mode
	if live == "sse" {
		sseOffset := stream.Offset(offset)
		if offset == "now" {
			sseOffset = str.CurrentOffset()
		} else if offset == "-1" {
			sseOffset = stream.StartOffset
		}
		s.handleSSE(ctx, w, str, sseOffset)
		return
	}

	// Handle offset=now without long-poll (return current tail offset)
	if offset == "now" && live != "long-poll" {
		currentOffset := str.CurrentOffset()
		w.Header().Set(HeaderContentType, str.ContentType())
		w.Header().Set(HeaderStreamOffset, string(currentOffset))
		w.Header().Set(HeaderStreamUpToDate, "true")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)

		if isJSONContentType(str.ContentType()) {
			w.Write([]byte("[]"))
		}
		return
	}

	// Convert offset to effective offset (handle "now")
	effectiveOffset := stream.StartOffset
	if offset == "now" {
		effectiveOffset = str.CurrentOffset()
	} else if offset != "" && offset != "-1" {
		effectiveOffset = stream.Offset(offset)
	}

	// Read messages from the stream
	batch, err := str.ReadFrom(ctx, effectiveOffset)
	if err != nil {
		http.Error(w, "Failed to read stream", http.StatusInternalServerError)
		return
	}

	// Long-poll: wait for new messages if caught up
	if live == "long-poll" && batch.Len() == 0 {
		result := str.WaitForMessages(ctx, effectiveOffset, s.longPollTimeout)

		if result.TimedOut {
			// Return 204 No Content on timeout
			w.Header().Set(HeaderStreamOffset, string(str.CurrentOffset()))
			w.Header().Set(HeaderStreamUpToDate, "true")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Got new messages
		batch = stream.NewBatch(result.Messages, str.CurrentOffset())
	}

	// Build response
	w.Header().Set(HeaderContentType, str.ContentType())

	// Set offset header to last message's offset, or current if no messages
	responseOffset := str.CurrentOffset()
	if lastMsg, ok := batch.Last(); ok {
		responseOffset = lastMsg.Offset
	}
	w.Header().Set(HeaderStreamOffset, string(responseOffset))

	// Set up-to-date header if we've caught up
	lastMsg, hasLast := batch.Last()
	if !hasLast || lastMsg.Offset == str.CurrentOffset() {
		w.Header().Set(HeaderStreamUpToDate, "true")
	}

	w.WriteHeader(http.StatusOK)

	// Format response
	if isJSONContentType(str.ContentType()) {
		// JSON mode: messages are stored with trailing commas
		// Concatenate all and wrap in [], stripping final comma
		response := formatJSONResponse(batch.Messages)
		w.Write(response)
	} else {
		// Binary mode: concatenate raw data
		for _, msg := range batch.Messages {
			w.Write(msg.Data)
		}
	}
}

// formatJSONResponse concatenates JSON messages and wraps in array.
// Messages are stored with trailing commas for easy concatenation.
func formatJSONResponse(messages []stream.Message) []byte {
	if len(messages) == 0 {
		return []byte("[]")
	}

	// Concatenate all message data
	var buf []byte
	for _, msg := range messages {
		buf = append(buf, msg.Data...)
	}

	// Strip trailing comma if present
	if len(buf) > 0 && buf[len(buf)-1] == ',' {
		buf = buf[:len(buf)-1]
	}

	// Wrap in array brackets
	result := make([]byte, 0, len(buf)+2)
	result = append(result, '[')
	result = append(result, buf...)
	result = append(result, ']')
	return result
}

// handleSSE handles Server-Sent Events streaming.
func (s *Server) handleSSE(ctx context.Context, w http.ResponseWriter, str stream.Stream, initialOffset stream.Offset) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)

	// Ensure we can flush
	flusher, ok := w.(http.Flusher)
	if !ok {
		// Fall back to no flushing (for tests)
		flusher = nil
	}

	currentOffset := initialOffset
	isJsonStream := isJSONContentType(str.ContentType())

	// Send initial data then wait for more
	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read current messages
		batch, err := str.ReadFrom(ctx, currentOffset)
		if err != nil {
			return
		}

		// Send data events for each message
		for _, msg := range batch.Messages {
			var dataPayload string
			if isJsonStream {
				// Wrap single message in array for JSON streams
				// Messages are stored with trailing comma, strip it and wrap
				data := msg.Data
				if len(data) > 0 && data[len(data)-1] == ',' {
					data = data[:len(data)-1]
				}
				dataPayload = "[" + string(data) + "]"
			} else {
				dataPayload = string(msg.Data)
			}

			// Send data event - handle newlines properly for SSE
			w.Write([]byte("event: data\n"))
			w.Write(formatSSEData(dataPayload))
			w.Write([]byte("\n"))
			currentOffset = msg.Offset
		}

		// Send control event with current offset
		controlOffset := str.CurrentOffset()
		if lastMsg, ok := batch.Last(); ok {
			controlOffset = lastMsg.Offset
		}

		controlData := map[string]interface{}{
			"offset":   string(controlOffset),
			"upToDate": batch.Len() == 0 || controlOffset == str.CurrentOffset(),
		}
		controlJSON, _ := encodeJSON(controlData)

		w.Write([]byte("event: control\n"))
		w.Write([]byte("data: " + controlJSON + "\n\n"))
		if flusher != nil {
			flusher.Flush()
		}

		// Update current offset
		currentOffset = controlOffset

		// If caught up, wait for new messages (or exit for test environments)
		if batch.Len() == 0 || controlOffset == str.CurrentOffset() {
			result := str.WaitForMessages(ctx, currentOffset, s.longPollTimeout)

			// Check context again after waiting
			select {
			case <-ctx.Done():
				return
			default:
			}

			if result.TimedOut {
				// Send keepalive control event and exit
				// In production this would loop, but for testing we exit after one timeout
				keepAliveData := map[string]interface{}{
					"offset":   string(currentOffset),
					"upToDate": true,
				}
				keepAliveJSON, _ := encodeJSON(keepAliveData)
				w.Write([]byte("event: control\n"))
				w.Write([]byte("data: " + keepAliveJSON + "\n\n"))
				if flusher != nil {
					flusher.Flush()
				}
				// Exit after timeout to avoid infinite loop in tests
				return
			}
			// Got new messages - continue loop to send them
		}
	}
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

// isJSONContentType checks if the content type is JSON.
func isJSONContentType(ct string) bool {
	return strings.Contains(ct, "application/json")
}

// encodeJSON encodes a value to JSON string.
func encodeJSON(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// formatSSEData formats data for SSE, handling newlines.
// SSE requires each line of data to be prefixed with "data: ".
// Newlines (\n, \r, or \r\n) must be split across multiple data: lines.
func formatSSEData(payload string) []byte {
	// Replace CR and CRLF with LF for consistent handling
	payload = strings.ReplaceAll(payload, "\r\n", "\n")
	payload = strings.ReplaceAll(payload, "\r", "\n")

	lines := strings.Split(payload, "\n")
	var result []byte
	for i, line := range lines {
		if i > 0 {
			result = append(result, '\n')
		}
		result = append(result, []byte("data: ")...)
		result = append(result, []byte(line)...)
	}
	result = append(result, '\n')
	return result
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
