package stream

// Batch represents a collection of messages from a stream read operation.
// Batches are returned from stream reads and contain zero or more messages
// along with metadata about the read position.
type Batch struct {
	// Messages contains the messages in this batch.
	Messages []Message

	// NextOffset is the offset to use for the next read operation.
	// This is the position after the last message in the batch.
	NextOffset Offset
}

// NewBatch creates a new batch with the given messages and next offset.
// The messages slice is copied to ensure immutability.
func NewBatch(messages []Message, nextOffset Offset) Batch {
	// Copy messages to ensure immutability
	messagesCopy := make([]Message, len(messages))
	copy(messagesCopy, messages)

	return Batch{
		Messages:   messagesCopy,
		NextOffset: nextOffset,
	}
}

// Len returns the number of messages in the batch.
func (b Batch) Len() int {
	return len(b.Messages)
}

// IsEmpty returns true if the batch contains no messages.
func (b Batch) IsEmpty() bool {
	return len(b.Messages) == 0
}

// TotalSize returns the total size of all message data in bytes.
func (b Batch) TotalSize() int {
	total := 0
	for _, msg := range b.Messages {
		total += msg.Size()
	}
	return total
}

// First returns the first message in the batch, if any.
// Returns the message and true if the batch is non-empty,
// or a zero Message and false if the batch is empty.
func (b Batch) First() (Message, bool) {
	if len(b.Messages) == 0 {
		return Message{}, false
	}
	return b.Messages[0], true
}

// Last returns the last message in the batch, if any.
// Returns the message and true if the batch is non-empty,
// or a zero Message and false if the batch is empty.
func (b Batch) Last() (Message, bool) {
	if len(b.Messages) == 0 {
		return Message{}, false
	}
	return b.Messages[len(b.Messages)-1], true
}
