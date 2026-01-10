package stream

import (
	"testing"
	"time"
)

func TestBatch_NewBatch(t *testing.T) {
	messages := []Message{
		NewMessage([]byte("msg1"), Offset("1_4"), time.Now()),
		NewMessage([]byte("msg2"), Offset("1_8"), time.Now()),
	}
	nextOffset := Offset("1_8")

	batch := NewBatch(messages, nextOffset)

	if len(batch.Messages) != 2 {
		t.Errorf("len(Messages) = %d, want 2", len(batch.Messages))
	}
	if batch.NextOffset != nextOffset {
		t.Errorf("NextOffset = %q, want %q", batch.NextOffset, nextOffset)
	}
}

func TestBatch_EmptyBatch(t *testing.T) {
	batch := NewBatch(nil, Offset("1_0"))

	if len(batch.Messages) != 0 {
		t.Errorf("len(Messages) = %d, want 0", len(batch.Messages))
	}
	if !batch.IsEmpty() {
		t.Error("IsEmpty() = false, want true")
	}
}

func TestBatch_Len(t *testing.T) {
	tests := []struct {
		name     string
		messages []Message
		want     int
	}{
		{"nil", nil, 0},
		{"empty", []Message{}, 0},
		{"one", []Message{NewMessage([]byte("x"), Offset("1_1"), time.Now())}, 1},
		{"three", []Message{
			NewMessage([]byte("a"), Offset("1_1"), time.Now()),
			NewMessage([]byte("b"), Offset("1_2"), time.Now()),
			NewMessage([]byte("c"), Offset("1_3"), time.Now()),
		}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := NewBatch(tt.messages, Offset("1_0"))
			if got := batch.Len(); got != tt.want {
				t.Errorf("Len() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBatch_TotalSize(t *testing.T) {
	tests := []struct {
		name     string
		messages []Message
		want     int
	}{
		{"empty", nil, 0},
		{"one message", []Message{
			NewMessage([]byte("hello"), Offset("1_5"), time.Now()),
		}, 5},
		{"multiple messages", []Message{
			NewMessage([]byte("hello"), Offset("1_5"), time.Now()),
			NewMessage([]byte("world!"), Offset("1_11"), time.Now()),
		}, 11},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := NewBatch(tt.messages, Offset("1_0"))
			if got := batch.TotalSize(); got != tt.want {
				t.Errorf("TotalSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBatch_First(t *testing.T) {
	msg1 := NewMessage([]byte("first"), Offset("1_5"), time.Now())
	msg2 := NewMessage([]byte("second"), Offset("1_11"), time.Now())

	batch := NewBatch([]Message{msg1, msg2}, Offset("1_11"))

	first, ok := batch.First()
	if !ok {
		t.Fatal("First() returned false, want true")
	}
	if string(first.Data) != "first" {
		t.Errorf("First().Data = %q, want %q", first.Data, "first")
	}
}

func TestBatch_First_Empty(t *testing.T) {
	batch := NewBatch(nil, Offset("1_0"))

	_, ok := batch.First()
	if ok {
		t.Error("First() returned true for empty batch, want false")
	}
}

func TestBatch_Last(t *testing.T) {
	msg1 := NewMessage([]byte("first"), Offset("1_5"), time.Now())
	msg2 := NewMessage([]byte("second"), Offset("1_11"), time.Now())

	batch := NewBatch([]Message{msg1, msg2}, Offset("1_11"))

	last, ok := batch.Last()
	if !ok {
		t.Fatal("Last() returned false, want true")
	}
	if string(last.Data) != "second" {
		t.Errorf("Last().Data = %q, want %q", last.Data, "second")
	}
}

func TestBatch_Last_Empty(t *testing.T) {
	batch := NewBatch(nil, Offset("1_0"))

	_, ok := batch.Last()
	if ok {
		t.Error("Last() returned true for empty batch, want false")
	}
}

func TestBatch_Immutability(t *testing.T) {
	original := []Message{
		NewMessage([]byte("msg1"), Offset("1_4"), time.Now()),
	}
	batch := NewBatch(original, Offset("1_4"))

	// Modify original slice
	original[0] = NewMessage([]byte("modified"), Offset("2_8"), time.Now())

	// Batch should be unchanged
	if string(batch.Messages[0].Data) != "msg1" {
		t.Error("Batch was modified when original slice was changed")
	}
}
