package stream

import (
	"testing"
	"time"
)

func TestMessage_NewMessage(t *testing.T) {
	data := []byte("hello world")
	offset := Offset("1_11")
	timestamp := time.Now()

	msg := NewMessage(data, offset, timestamp)

	if string(msg.Data) != "hello world" {
		t.Errorf("Data = %q, want %q", msg.Data, "hello world")
	}
	if msg.Offset != offset {
		t.Errorf("Offset = %q, want %q", msg.Offset, offset)
	}
	if !msg.Timestamp.Equal(timestamp) {
		t.Errorf("Timestamp = %v, want %v", msg.Timestamp, timestamp)
	}
}

func TestMessage_DataImmutability(t *testing.T) {
	// Ensure message data is a copy, not a reference
	original := []byte("original")
	msg := NewMessage(original, Offset("1_8"), time.Now())

	// Modify original
	original[0] = 'X'

	// Message data should be unchanged
	if string(msg.Data) != "original" {
		t.Error("Message data was modified when original slice was changed")
	}
}

func TestMessage_Size(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want int
	}{
		{"empty", []byte{}, 0},
		{"single byte", []byte{0x42}, 1},
		{"hello", []byte("hello"), 5},
		{"unicode", []byte("日本語"), 9}, // 3 bytes per character
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMessage(tt.data, Offset("1_0"), time.Now())
			if got := msg.Size(); got != tt.want {
				t.Errorf("Size() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMessage_IsEmpty(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"nil data", nil, true},
		{"empty slice", []byte{}, true},
		{"single byte", []byte{0x42}, false},
		{"string data", []byte("hello"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMessage(tt.data, Offset("1_0"), time.Now())
			if got := msg.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}
