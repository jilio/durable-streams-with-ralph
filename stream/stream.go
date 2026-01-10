package stream

import (
	"context"
	"strings"
	"time"
)

// Stream defines the interface for a durable stream.
// Implementations handle the actual storage and retrieval of messages.
type Stream interface {
	// Path returns the stream's URL path (identifier).
	Path() string

	// ContentType returns the MIME content type of the stream.
	ContentType() string

	// CurrentOffset returns the current tail offset (next write position).
	// This is the offset that would be assigned to the next appended message.
	CurrentOffset() Offset

	// Append writes data to the stream and returns the assigned offset.
	// The offset returned is the position after the appended data.
	// Returns an error if the append fails.
	Append(ctx context.Context, data []byte) (Offset, error)

	// ReadFrom reads messages starting after the given offset.
	// Use StartOffset (-1) to read from the beginning.
	// Returns a Batch containing zero or more messages.
	// The batch's NextOffset should be used for subsequent reads.
	ReadFrom(ctx context.Context, offset Offset) (Batch, error)

	// WaitForMessages blocks until new messages are available after the given offset,
	// or until the timeout expires. Returns WaitResult indicating what happened.
	WaitForMessages(ctx context.Context, offset Offset, timeout time.Duration) WaitResult
}

// WaitResult represents the result of a WaitForMessages call.
type WaitResult struct {
	// Messages contains any new messages received.
	Messages []Message

	// TimedOut is true if the wait timed out without receiving messages.
	TimedOut bool
}

// StreamConfig holds configuration for creating a new stream.
type StreamConfig struct {
	// Path is the stream's URL path (identifier).
	Path string

	// ContentType is the MIME content type for the stream.
	// Defaults to "application/octet-stream" if empty.
	ContentType string

	// TTLSeconds is the time-to-live in seconds.
	// Zero means no TTL (stream persists until deleted).
	TTLSeconds int

	// ExpiresAt is an absolute expiry time (RFC 3339 format).
	// Empty string means no expiry.
	ExpiresAt string
}

// DefaultContentType is the default MIME type for streams.
const DefaultContentType = "application/octet-stream"

// NormalizeContentType normalizes a content-type string for comparison.
// It strips charset parameters and converts to lowercase.
func NormalizeContentType(ct string) string {
	if ct == "" {
		return ""
	}
	// Split on semicolon to strip charset
	parts := strings.SplitN(ct, ";", 2)
	return strings.ToLower(strings.TrimSpace(parts[0]))
}

// Normalize fills in default values for the stream config.
func (c *StreamConfig) Normalize() {
	if c.ContentType == "" {
		c.ContentType = DefaultContentType
	}
}

// NormalizedContentType returns the normalized content type for comparison.
func (c *StreamConfig) NormalizedContentType() string {
	return NormalizeContentType(c.ContentType)
}

// ConfigMatches checks if two configs are compatible for idempotent PUT.
// Returns true if they match (same normalized content-type, TTL, and ExpiresAt).
func (c *StreamConfig) ConfigMatches(other *StreamConfig) bool {
	return c.NormalizedContentType() == other.NormalizedContentType() &&
		c.TTLSeconds == other.TTLSeconds &&
		c.ExpiresAt == other.ExpiresAt
}
