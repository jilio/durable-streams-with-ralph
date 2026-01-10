package stream

import (
	"context"
	"errors"
)

// Common errors for stream storage operations.
var (
	// ErrStreamExists is returned when attempting to create a stream that already exists.
	ErrStreamExists = errors.New("stream already exists")

	// ErrStreamNotFound is returned when a stream does not exist.
	ErrStreamNotFound = errors.New("stream not found")

	// ErrOffsetGone is returned when the requested offset is no longer available
	// (e.g., due to retention policies).
	ErrOffsetGone = errors.New("offset is no longer available")

	// ErrContentTypeMismatch is returned when appending with a content type
	// that doesn't match the stream's configured content type.
	ErrContentTypeMismatch = errors.New("content type mismatch")

	// ErrEmptyAppend is returned when attempting to append empty data.
	ErrEmptyAppend = errors.New("cannot append empty data")

	// ErrSeqConflict is returned when a sequence number conflicts with
	// an existing sequence (for writer coordination).
	ErrSeqConflict = errors.New("sequence conflict")
)

// StreamStorage defines the interface for stream storage backends.
// Implementations handle the actual persistence and retrieval of streams.
type StreamStorage interface {
	// Create creates a new stream with the given configuration.
	// Returns ErrStreamExists if a stream already exists at the path.
	Create(ctx context.Context, config StreamConfig) error

	// Get returns a Stream handle for the given path.
	// Returns ErrStreamNotFound if the stream doesn't exist.
	Get(ctx context.Context, path string) (Stream, error)

	// Delete removes a stream and all its data.
	// Returns ErrStreamNotFound if the stream doesn't exist.
	Delete(ctx context.Context, path string) error

	// Exists checks if a stream exists at the given path.
	Exists(ctx context.Context, path string) (bool, error)

	// Head returns metadata about a stream without reading its content.
	// Returns ErrStreamNotFound if the stream doesn't exist.
	Head(ctx context.Context, path string) (*StreamMetadata, error)
}

// StreamMetadata contains information about a stream.
// This is returned by the Head operation.
type StreamMetadata struct {
	// Path is the stream's URL path (identifier).
	Path string

	// ContentType is the MIME content type of the stream.
	ContentType string

	// NextOffset is the tail offset (next position after current end).
	NextOffset Offset

	// TTLSeconds is the remaining time-to-live in seconds.
	// Zero means no TTL.
	TTLSeconds int

	// ExpiresAt is the absolute expiry time (RFC 3339 format).
	// Empty string means no expiry.
	ExpiresAt string

	// CreatedAt is when the stream was created (Unix timestamp milliseconds).
	CreatedAt int64
}
