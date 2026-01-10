package stream

import "context"

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

// Normalize fills in default values for the stream config.
func (c *StreamConfig) Normalize() {
	if c.ContentType == "" {
		c.ContentType = DefaultContentType
	}
}
