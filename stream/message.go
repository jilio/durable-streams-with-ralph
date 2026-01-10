package stream

import "time"

// Message represents a single message in a stream.
// Messages are immutable once created.
type Message struct {
	// Data is the raw bytes of the message.
	Data []byte

	// Offset is the position after this message in the stream.
	// Format is implementation-defined but lexicographically sortable.
	Offset Offset

	// Timestamp is when the message was appended to the stream.
	Timestamp time.Time
}

// NewMessage creates a new message with the given data, offset, and timestamp.
// The data is copied to ensure immutability.
func NewMessage(data []byte, offset Offset, timestamp time.Time) Message {
	// Copy data to ensure immutability
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return Message{
		Data:      dataCopy,
		Offset:    offset,
		Timestamp: timestamp,
	}
}

// Size returns the size of the message data in bytes.
func (m Message) Size() int {
	return len(m.Data)
}

// IsEmpty returns true if the message has no data.
func (m Message) IsEmpty() bool {
	return len(m.Data) == 0
}
