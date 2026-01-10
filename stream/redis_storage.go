package stream

/*
Redis Storage Design for Durable Streams

Redis Streams provide a natural fit for durable streams since they are:
- Append-only logs with unique IDs
- Support blocking reads (XREAD BLOCK)
- Have built-in consumer groups (not needed for our use case)
- Efficient for both reads and writes

## Key Naming Convention

All keys are prefixed with "ds:" (durable-streams):

1. Stream data: "ds:stream:{path}"
   - Uses Redis Streams (XADD/XREAD/XRANGE)
   - Path is URL-encoded to handle special characters
   - Example: "ds:stream:/v1/stream/events" → "ds:stream:%2Fv1%2Fstream%2Fevents"

2. Stream metadata: "ds:meta:{path}"
   - Uses Redis Hash
   - Fields: content_type, ttl_seconds, expires_at, created_at
   - Example: "ds:meta:%2Fv1%2Fstream%2Fevents"

3. Producer state: "ds:producer:{path}:{producer_id}"
   - Uses Redis Hash
   - Fields: epoch, last_seq, last_updated
   - Example: "ds:producer:%2Fv1%2Fstream%2Fevents:producer-123"

4. Stream-Seq tracking: "ds:seq:{path}"
   - Uses Redis String
   - Stores the last accepted Stream-Seq value
   - Example: "ds:seq:%2Fv1%2Fstream%2Fevents"

## Offset Mapping

Durable Streams uses a custom offset format: "SSSSSSSSSSSSSSSS_LLLLLLLLLLLLLLLL"
where S is segment (16 hex) and L is logical position (16 hex).

Redis Streams use: "TTTTTTTTTTTT-SSSS" (milliseconds-sequence)

Mapping strategy:
- Store our offset in the message payload, not the Redis ID
- Use Redis auto-generated IDs ("*") for ordering
- Our offset tracks byte position, Redis ID tracks time

Message format in Redis Stream:
  Field "d" = message data (bytes)
  Field "o" = durable-streams offset (string)
  Field "t" = timestamp (unix ms)

## TTL/Expiration

- Use Redis EXPIRE on the stream key for TTL
- Use Redis EXPIREAT for absolute ExpiresAt times
- Set expiration on both stream and metadata keys

## Long-Poll Implementation

Use XREAD BLOCK for efficient long-polling:
- XREAD BLOCK <timeout_ms> STREAMS ds:stream:{path} <last_redis_id>
- Returns immediately if new data, or blocks until timeout

## Command Pipelining

For performance, pipeline multiple commands:
- Batch XADD commands for high-throughput writes
- Pipeline HGET + XRANGE for metadata + data reads

## Connection Pooling

Use go-redis's built-in connection pool:
- PoolSize: runtime.NumCPU() * 10
- MinIdleConns: runtime.NumCPU()
- PoolTimeout: 4 seconds
*/

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Redis key prefixes
	redisKeyPrefix      = "ds:"
	redisStreamPrefix   = "ds:stream:"
	redisMetaPrefix     = "ds:meta:"
	redisProducerPrefix = "ds:producer:"
	redisSeqPrefix      = "ds:seq:"

	// Redis hash fields for metadata
	redisFieldContentType = "content_type"
	redisFieldTTLSeconds  = "ttl_seconds"
	redisFieldExpiresAt   = "expires_at"
	redisFieldCreatedAt   = "created_at"

	// Redis hash fields for producer state
	redisFieldEpoch       = "epoch"
	redisFieldLastSeq     = "last_seq"
	redisFieldLastUpdated = "last_updated"

	// Redis stream message fields
	redisFieldData      = "d" // message data (base64 encoded for binary safety)
	redisFieldOffset    = "o" // durable-streams offset
	redisFieldTimestamp = "t" // timestamp (unix ms)
)

// RedisStorage implements StreamStorage using Redis Streams.
type RedisStorage struct {
	client *redis.Client
}

// RedisStorageConfig holds configuration for Redis storage.
type RedisStorageConfig struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string

	// Password for Redis authentication (optional)
	Password string

	// DB is the Redis database number (default 0)
	DB int

	// PoolSize is the maximum number of connections (default: 10 * NumCPU)
	PoolSize int

	// MinIdleConns is the minimum number of idle connections (default: NumCPU)
	MinIdleConns int
}

// NewRedisStorage creates a new Redis-backed storage.
func NewRedisStorage(cfg RedisStorageConfig) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisStorage{client: client}, nil
}

// Close closes the Redis connection.
func (s *RedisStorage) Close() error {
	return s.client.Close()
}

// Helper functions for key generation

func encodePathForRedis(path string) string {
	return url.PathEscape(path)
}

func streamKey(path string) string {
	return redisStreamPrefix + encodePathForRedis(path)
}

func metaKey(path string) string {
	return redisMetaPrefix + encodePathForRedis(path)
}

func producerKey(path, producerID string) string {
	return redisProducerPrefix + encodePathForRedis(path) + ":" + producerID
}

func seqKey(path string) string {
	return redisSeqPrefix + encodePathForRedis(path)
}

// encodeData encodes binary data for safe storage in Redis
func encodeData(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// decodeData decodes data from Redis storage
func decodeData(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

// parseRedisID extracts timestamp and sequence from Redis Stream ID (e.g., "1234567890-0")
func parseRedisID(id string) (int64, int64, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid Redis ID format: %s", id)
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid timestamp in Redis ID: %w", err)
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sequence in Redis ID: %w", err)
	}
	return ts, seq, nil
}

// Create creates a new stream with the given configuration.
func (s *RedisStorage) Create(ctx context.Context, config StreamConfig) error {
	config.Normalize()

	mk := metaKey(config.Path)

	// Check if stream already exists
	exists, err := s.client.Exists(ctx, mk).Result()
	if err != nil {
		return fmt.Errorf("failed to check stream existence: %w", err)
	}

	if exists > 0 {
		// Check if config matches for idempotent PUT
		existingConfig, err := s.getConfig(ctx, config.Path)
		if err != nil {
			return err
		}
		if config.ConfigMatches(existingConfig) {
			return ErrStreamExists // Idempotent success
		}
		return ErrConfigMismatch // Config conflict
	}

	// Create metadata hash
	now := time.Now().UnixMilli()
	fields := map[string]interface{}{
		redisFieldContentType: config.ContentType,
		redisFieldTTLSeconds:  config.TTLSeconds,
		redisFieldExpiresAt:   config.ExpiresAt,
		redisFieldCreatedAt:   now,
	}

	if err := s.client.HSet(ctx, mk, fields).Err(); err != nil {
		return fmt.Errorf("failed to create stream metadata: %w", err)
	}

	// Set expiration if TTL or ExpiresAt is specified
	if err := s.setExpiration(ctx, config.Path, config.TTLSeconds, config.ExpiresAt, now); err != nil {
		// Clean up on error
		s.client.Del(ctx, mk)
		return err
	}

	return nil
}

// getConfig retrieves the stream configuration from metadata.
func (s *RedisStorage) getConfig(ctx context.Context, path string) (*StreamConfig, error) {
	mk := metaKey(path)
	result, err := s.client.HGetAll(ctx, mk).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream config: %w", err)
	}
	if len(result) == 0 {
		return nil, ErrStreamNotFound
	}

	ttl, _ := strconv.Atoi(result[redisFieldTTLSeconds])
	return &StreamConfig{
		Path:        path,
		ContentType: result[redisFieldContentType],
		TTLSeconds:  ttl,
		ExpiresAt:   result[redisFieldExpiresAt],
	}, nil
}

// setExpiration sets TTL or absolute expiration on stream keys.
func (s *RedisStorage) setExpiration(ctx context.Context, path string, ttlSeconds int, expiresAt string, createdAt int64) error {
	mk := metaKey(path)
	sk := streamKey(path)

	if ttlSeconds > 0 {
		ttl := time.Duration(ttlSeconds) * time.Second
		s.client.Expire(ctx, mk, ttl)
		s.client.Expire(ctx, sk, ttl)
	} else if expiresAt != "" {
		expTime, err := time.Parse(time.RFC3339, expiresAt)
		if err != nil {
			return fmt.Errorf("invalid ExpiresAt format: %w", err)
		}
		s.client.ExpireAt(ctx, mk, expTime)
		s.client.ExpireAt(ctx, sk, expTime)
	}
	return nil
}

// Get returns a Stream handle for the given path.
func (s *RedisStorage) Get(ctx context.Context, path string) (Stream, error) {
	mk := metaKey(path)
	exists, err := s.client.Exists(ctx, mk).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to check stream existence: %w", err)
	}
	if exists == 0 {
		return nil, ErrStreamNotFound
	}

	config, err := s.getConfig(ctx, path)
	if err != nil {
		return nil, err
	}

	return &redisStream{
		storage: s,
		path:    path,
		config:  config,
	}, nil
}

// Delete removes a stream and all its data.
func (s *RedisStorage) Delete(ctx context.Context, path string) error {
	mk := metaKey(path)
	sk := streamKey(path)
	sqk := seqKey(path)

	// Check if stream exists
	exists, err := s.client.Exists(ctx, mk).Result()
	if err != nil {
		return fmt.Errorf("failed to check stream existence: %w", err)
	}
	if exists == 0 {
		return ErrStreamNotFound
	}

	// Delete all keys (metadata, stream data, seq tracker)
	// Also delete any producer keys (pattern match would require SCAN, so we just delete known keys)
	if err := s.client.Del(ctx, mk, sk, sqk).Err(); err != nil {
		return fmt.Errorf("failed to delete stream: %w", err)
	}

	return nil
}

// Exists checks if a stream exists at the given path.
func (s *RedisStorage) Exists(ctx context.Context, path string) (bool, error) {
	mk := metaKey(path)
	exists, err := s.client.Exists(ctx, mk).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check stream existence: %w", err)
	}
	return exists > 0, nil
}

// Head returns metadata about a stream without reading its content.
func (s *RedisStorage) Head(ctx context.Context, path string) (*StreamMetadata, error) {
	mk := metaKey(path)
	sk := streamKey(path)

	// Get metadata
	result, err := s.client.HGetAll(ctx, mk).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream metadata: %w", err)
	}
	if len(result) == 0 {
		return nil, ErrStreamNotFound
	}

	// Get the last message to determine current offset
	// XREVRANGE key + - COUNT 1 gets the last entry
	entries, err := s.client.XRevRangeN(ctx, sk, "+", "-", 1).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get stream offset: %w", err)
	}

	var nextOffset Offset = "0_0"
	if len(entries) > 0 {
		// Get the offset from the last message
		if offsetStr, ok := entries[0].Values[redisFieldOffset].(string); ok {
			nextOffset = Offset(offsetStr)
		}
	}

	ttl, _ := strconv.Atoi(result[redisFieldTTLSeconds])
	createdAt, _ := strconv.ParseInt(result[redisFieldCreatedAt], 10, 64)

	return &StreamMetadata{
		Path:        path,
		ContentType: result[redisFieldContentType],
		NextOffset:  nextOffset,
		TTLSeconds:  ttl,
		ExpiresAt:   result[redisFieldExpiresAt],
		CreatedAt:   createdAt,
	}, nil
}

// redisStream implements Stream interface for Redis-backed streams.
type redisStream struct {
	storage   *RedisStorage
	path      string
	config    *StreamConfig
	offsetGen *OffsetGenerator
}

func (r *redisStream) Path() string {
	return r.path
}

func (r *redisStream) ContentType() string {
	return r.config.ContentType
}

func (r *redisStream) CurrentOffset() Offset {
	ctx := context.Background()
	sk := streamKey(r.path)

	// Get the last entry to find current offset
	entries, err := r.storage.client.XRevRangeN(ctx, sk, "+", "-", 1).Result()
	if err != nil || len(entries) == 0 {
		return Offset("0_0")
	}

	if offsetStr, ok := entries[0].Values[redisFieldOffset].(string); ok {
		return Offset(offsetStr)
	}
	return Offset("0_0")
}

func (r *redisStream) Append(ctx context.Context, data []byte) (Offset, error) {
	sk := streamKey(r.path)

	// Generate offset - need to track position
	// For simplicity, we'll use a counter stored in Redis or calculate from stream length
	if r.offsetGen == nil {
		// Initialize from current stream state
		currentOffset := r.CurrentOffset()
		if currentOffset != "0_0" && currentOffset != "" {
			r.offsetGen = NewOffsetGeneratorFrom(currentOffset)
		} else {
			r.offsetGen = NewOffsetGenerator()
		}
	}

	offset := r.offsetGen.Next(len(data))
	now := time.Now().UnixMilli()

	// Add to Redis Stream
	args := &redis.XAddArgs{
		Stream: sk,
		Values: map[string]interface{}{
			redisFieldData:      encodeData(data),
			redisFieldOffset:    string(offset),
			redisFieldTimestamp: now,
		},
	}

	_, err := r.storage.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("failed to append to stream: %w", err)
	}

	return offset, nil
}

func (r *redisStream) AppendWithSeq(ctx context.Context, data []byte, seq string) (Offset, error) {
	if seq != "" {
		sqk := seqKey(r.path)

		// Check sequence ordering using GETSET pattern
		lastSeq, err := r.storage.client.Get(ctx, sqk).Result()
		if err != nil && err != redis.Nil {
			return "", fmt.Errorf("failed to get last seq: %w", err)
		}

		if lastSeq != "" && seq <= lastSeq {
			return "", ErrSeqConflict
		}

		// Append the data
		offset, err := r.Append(ctx, data)
		if err != nil {
			return "", err
		}

		// Update the seq tracker
		if err := r.storage.client.Set(ctx, sqk, seq, 0).Err(); err != nil {
			// Note: data was already appended, seq update failed
			// In production, this should be atomic
			return offset, nil
		}

		return offset, nil
	}

	return r.Append(ctx, data)
}

func (r *redisStream) AppendWithProducer(ctx context.Context, data []byte, producerId string, epoch, seq int64) (Offset, *ProducerResult) {
	pk := producerKey(r.path, producerId)
	now := time.Now().UnixMilli()

	// Get current producer state
	state, err := r.storage.client.HGetAll(ctx, pk).Result()
	if err != nil && err != redis.Nil {
		return "", &ProducerResult{Status: ProducerStatusSequenceGap}
	}

	if len(state) == 0 {
		// New producer - must start at seq=0
		if seq != 0 {
			return "", &ProducerResult{
				Status:      ProducerStatusSequenceGap,
				ExpectedSeq: 0,
				ReceivedSeq: seq,
			}
		}

		// Accept new producer
		offset, err := r.Append(ctx, data)
		if err != nil {
			return "", &ProducerResult{Status: ProducerStatusSequenceGap}
		}

		// Store producer state
		r.storage.client.HSet(ctx, pk, map[string]interface{}{
			redisFieldEpoch:       epoch,
			redisFieldLastSeq:     0,
			redisFieldLastUpdated: now,
		})

		return offset, &ProducerResult{Status: ProducerStatusAccepted}
	}

	// Existing producer - validate epoch and sequence
	currentEpoch, _ := strconv.ParseInt(state[redisFieldEpoch], 10, 64)
	lastSeq, _ := strconv.ParseInt(state[redisFieldLastSeq], 10, 64)

	if epoch < currentEpoch {
		return "", &ProducerResult{
			Status:       ProducerStatusStaleEpoch,
			CurrentEpoch: currentEpoch,
		}
	}

	if epoch > currentEpoch {
		// New epoch must start at seq=0
		if seq != 0 {
			return "", &ProducerResult{Status: ProducerStatusInvalidEpochSeq}
		}

		// Accept new epoch
		offset, err := r.Append(ctx, data)
		if err != nil {
			return "", &ProducerResult{Status: ProducerStatusSequenceGap}
		}

		r.storage.client.HSet(ctx, pk, map[string]interface{}{
			redisFieldEpoch:       epoch,
			redisFieldLastSeq:     0,
			redisFieldLastUpdated: now,
		})

		return offset, &ProducerResult{Status: ProducerStatusAccepted}
	}

	// Same epoch - check sequence
	if seq <= lastSeq {
		return "", &ProducerResult{
			Status:      ProducerStatusDuplicate,
			IsDuplicate: true,
			LastSeq:     lastSeq,
		}
	}

	if seq != lastSeq+1 {
		return "", &ProducerResult{
			Status:      ProducerStatusSequenceGap,
			ExpectedSeq: lastSeq + 1,
			ReceivedSeq: seq,
		}
	}

	// Accept next sequence
	offset, err := r.Append(ctx, data)
	if err != nil {
		return "", &ProducerResult{Status: ProducerStatusSequenceGap}
	}

	r.storage.client.HSet(ctx, pk, map[string]interface{}{
		redisFieldLastSeq:     seq,
		redisFieldLastUpdated: now,
	})

	return offset, &ProducerResult{Status: ProducerStatusAccepted}
}

func (r *redisStream) ReadFrom(ctx context.Context, offset Offset) (Batch, error) {
	sk := streamKey(r.path)

	// Read all entries from the stream
	// For offset-based filtering, we need to read all and filter
	entries, err := r.storage.client.XRange(ctx, sk, "-", "+").Result()
	if err != nil {
		return Batch{}, fmt.Errorf("failed to read stream: %w", err)
	}

	var msgs []Message
	for _, entry := range entries {
		msgOffset := Offset(entry.Values[redisFieldOffset].(string))

		// Filter: include messages after the given offset
		if offset.IsStart() || msgOffset.Compare(offset) > 0 {
			dataStr := entry.Values[redisFieldData].(string)
			data, err := decodeData(dataStr)
			if err != nil {
				continue // Skip malformed entries
			}

			ts, _ := strconv.ParseInt(entry.Values[redisFieldTimestamp].(string), 10, 64)
			msg := NewMessage(data, msgOffset, time.UnixMilli(ts))
			msgs = append(msgs, msg)
		}
	}

	return NewBatch(msgs, r.CurrentOffset()), nil
}

func (r *redisStream) WaitForMessages(ctx context.Context, offset Offset, timeout time.Duration) WaitResult {
	// For now, implement simple polling (XREAD BLOCK will be added in task 3my.5)
	deadline := time.Now().Add(timeout)
	pollInterval := 10 * time.Millisecond

	for {
		batch, err := r.ReadFrom(ctx, offset)
		if err == nil && len(batch.Messages) > 0 {
			return WaitResult{Messages: batch.Messages, TimedOut: false}
		}

		if time.Now().After(deadline) {
			return WaitResult{TimedOut: true}
		}

		select {
		case <-ctx.Done():
			return WaitResult{TimedOut: true}
		case <-time.After(pollInterval):
			// Continue polling
		}
	}
}

// Ensure interfaces are implemented
var (
	_ StreamStorage = (*RedisStorage)(nil)
	_ Stream        = (*redisStream)(nil)
)
