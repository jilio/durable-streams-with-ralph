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

// Placeholder implementations - to be filled in subsequent tasks

func (s *RedisStorage) Create(ctx context.Context, config StreamConfig) error {
	// TODO: Implement in task 3my.2
	return fmt.Errorf("not implemented")
}

func (s *RedisStorage) Get(ctx context.Context, path string) (Stream, error) {
	// TODO: Implement in task 3my.2
	return nil, fmt.Errorf("not implemented")
}

func (s *RedisStorage) Delete(ctx context.Context, path string) error {
	// TODO: Implement in task 3my.2
	return fmt.Errorf("not implemented")
}

func (s *RedisStorage) Exists(ctx context.Context, path string) (bool, error) {
	// TODO: Implement in task 3my.2
	return false, fmt.Errorf("not implemented")
}

func (s *RedisStorage) Head(ctx context.Context, path string) (*StreamMetadata, error) {
	// TODO: Implement in task 3my.2
	return nil, fmt.Errorf("not implemented")
}

// Ensure RedisStorage implements StreamStorage
var _ StreamStorage = (*RedisStorage)(nil)
