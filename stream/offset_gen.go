package stream

import (
	"fmt"
	"strconv"
	"strings"
)

// InitialOffset is the starting offset for a new stream.
// Format: {readSeq}_{byteOffset} with 16-digit zero-padding.
const InitialOffset Offset = "0000000000000000_0000000000000000"

// OffsetGenerator generates lexicographically sortable offsets.
// Offsets use the format: {readSeq}_{byteOffset} with 16-digit zero-padding.
type OffsetGenerator struct {
	readSeq    uint64
	byteOffset uint64
}

// NewOffsetGenerator creates a new offset generator starting from the initial offset.
func NewOffsetGenerator() *OffsetGenerator {
	return &OffsetGenerator{
		readSeq:    0,
		byteOffset: 0,
	}
}

// NewOffsetGeneratorFrom creates an offset generator starting from the given offset.
func NewOffsetGeneratorFrom(offset Offset) *OffsetGenerator {
	seq, byteOff, err := ParseOffset(offset)
	if err != nil {
		// If parsing fails, start from initial
		return NewOffsetGenerator()
	}
	return &OffsetGenerator{
		readSeq:    seq,
		byteOffset: byteOff,
	}
}

// Current returns the current offset without advancing.
func (g *OffsetGenerator) Current() Offset {
	return FormatOffset(g.readSeq, g.byteOffset)
}

// Next advances the offset by the given number of bytes and returns the new offset.
// This is the offset AFTER the data being appended.
func (g *OffsetGenerator) Next(bytes int) Offset {
	if bytes > 0 {
		g.byteOffset += uint64(bytes)
	}
	return g.Current()
}

// ParseOffset parses an offset string into its components.
// Returns readSeq, byteOffset, and any error.
func ParseOffset(offset Offset) (uint64, uint64, error) {
	s := string(offset)
	if s == "" {
		return 0, 0, fmt.Errorf("empty offset")
	}

	parts := strings.Split(s, "_")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid offset format: %q", s)
	}

	seq, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid read sequence: %w", err)
	}

	byteOff, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid byte offset: %w", err)
	}

	return seq, byteOff, nil
}

// FormatOffset creates an offset string from its components.
// Uses 16-digit zero-padding for lexicographic sorting.
func FormatOffset(readSeq, byteOffset uint64) Offset {
	return Offset(fmt.Sprintf("%016d_%016d", readSeq, byteOffset))
}
