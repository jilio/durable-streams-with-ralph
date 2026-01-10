package stream

import "strings"

// Offset is an opaque position token in a stream.
//
// Offsets are:
//   - Opaque: Do not parse or interpret offset structure
//   - Lexicographically sortable: Compare offsets to determine ordering
//   - Persistent: Valid for the stream's lifetime
//   - Unique: Each position has exactly one offset
//
// Use StartOffset to read from the beginning of a stream.
// Use NowOffset to read from the current tail position.
type Offset string

const (
	// StartOffset represents the beginning of a stream.
	// Use this to read from the start.
	StartOffset Offset = "-1"

	// NowOffset represents the current tail position.
	// Use this to skip all existing data and read only future data.
	NowOffset Offset = "now"
)

// String returns the offset as a string.
func (o Offset) String() string {
	return string(o)
}

// IsStart returns true if this offset represents the start of stream.
// Both empty string and "-1" are treated as start.
func (o Offset) IsStart() bool {
	return o == StartOffset || o == ""
}

// IsNow returns true if this offset represents the current tail position.
func (o Offset) IsNow() bool {
	return o == NowOffset
}

// IsSentinel returns true if this offset is a sentinel value (-1, now, or empty).
// Sentinel values have special meaning and are not actual stream positions.
func (o Offset) IsSentinel() bool {
	return o.IsStart() || o.IsNow()
}

// Compare compares two offsets lexicographically.
// Returns -1 if o < other, 0 if equal, 1 if o > other.
// This is used for determining ordering of offsets within a stream.
func (o Offset) Compare(other Offset) int {
	return strings.Compare(string(o), string(other))
}
