package stream

import (
	"testing"
)

func TestRedisKeyGeneration(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantKey  string
		keyFunc  func(string) string
	}{
		{
			name:    "stream key simple path",
			path:    "/test",
			wantKey: "ds:stream:%2Ftest",
			keyFunc: streamKey,
		},
		{
			name:    "stream key nested path",
			path:    "/v1/stream/events",
			wantKey: "ds:stream:%2Fv1%2Fstream%2Fevents",
			keyFunc: streamKey,
		},
		{
			name:    "meta key",
			path:    "/test",
			wantKey: "ds:meta:%2Ftest",
			keyFunc: metaKey,
		},
		{
			name:    "seq key",
			path:    "/test",
			wantKey: "ds:seq:%2Ftest",
			keyFunc: seqKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.keyFunc(tt.path)
			if got != tt.wantKey {
				t.Errorf("%s(%q) = %q, want %q", tt.name, tt.path, got, tt.wantKey)
			}
		})
	}
}

func TestProducerKeyGeneration(t *testing.T) {
	got := producerKey("/test", "producer-123")
	want := "ds:producer:%2Ftest:producer-123"
	if got != want {
		t.Errorf("producerKey() = %q, want %q", got, want)
	}
}

func TestDataEncoding(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"simple text", []byte("hello world")},
		{"binary data", []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}},
		{"unicode", []byte("こんにちは")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeData(tt.data)
			decoded, err := decodeData(encoded)
			if err != nil {
				t.Fatalf("decodeData() error = %v", err)
			}
			if string(decoded) != string(tt.data) {
				t.Errorf("round-trip failed: got %v, want %v", decoded, tt.data)
			}
		})
	}
}

func TestParseRedisID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantTs  int64
		wantSeq int64
		wantErr bool
	}{
		{
			name:    "valid ID",
			id:      "1234567890-5",
			wantTs:  1234567890,
			wantSeq: 5,
		},
		{
			name:    "zero sequence",
			id:      "1704067200000-0",
			wantTs:  1704067200000,
			wantSeq: 0,
		},
		{
			name:    "invalid format",
			id:      "invalid",
			wantErr: true,
		},
		{
			name:    "missing sequence",
			id:      "1234567890",
			wantErr: true,
		},
		{
			name:    "non-numeric timestamp",
			id:      "abc-0",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, seq, err := parseRedisID(tt.id)
			if tt.wantErr {
				if err == nil {
					t.Error("parseRedisID() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRedisID() error = %v", err)
			}
			if ts != tt.wantTs {
				t.Errorf("timestamp = %d, want %d", ts, tt.wantTs)
			}
			if seq != tt.wantSeq {
				t.Errorf("sequence = %d, want %d", seq, tt.wantSeq)
			}
		})
	}
}

func TestRedisStorageImplementsInterface(t *testing.T) {
	// Compile-time check that RedisStorage implements StreamStorage
	var _ StreamStorage = (*RedisStorage)(nil)
}
