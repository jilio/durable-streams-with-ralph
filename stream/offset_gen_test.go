package stream

import (
	"testing"
)

func TestOffsetGenerator_Initial(t *testing.T) {
	gen := NewOffsetGenerator()

	if got := gen.Current(); got != InitialOffset {
		t.Errorf("Current() = %q, want %q", got, InitialOffset)
	}
}

func TestOffsetGenerator_Next(t *testing.T) {
	gen := NewOffsetGenerator()

	// First append of 5 bytes
	offset := gen.Next(5)
	if offset != Offset("0000000000000000_0000000000000005") {
		t.Errorf("Next(5) = %q, want %q", offset, "0000000000000000_0000000000000005")
	}

	// Second append of 10 bytes
	offset = gen.Next(10)
	if offset != Offset("0000000000000000_0000000000000015") {
		t.Errorf("Next(10) = %q, want %q", offset, "0000000000000000_0000000000000015")
	}
}

func TestOffsetGenerator_Current(t *testing.T) {
	gen := NewOffsetGenerator()

	gen.Next(5)
	gen.Next(10)

	if got := gen.Current(); got != Offset("0000000000000000_0000000000000015") {
		t.Errorf("Current() = %q, want %q", got, "0000000000000000_0000000000000015")
	}
}

func TestOffsetGenerator_LexicographicOrdering(t *testing.T) {
	gen := NewOffsetGenerator()

	offsets := make([]Offset, 100)
	for i := 0; i < 100; i++ {
		offsets[i] = gen.Next(1)
	}

	// Verify lexicographic ordering
	for i := 1; i < len(offsets); i++ {
		if offsets[i-1].Compare(offsets[i]) >= 0 {
			t.Errorf("Offset %d (%q) >= Offset %d (%q)", i-1, offsets[i-1], i, offsets[i])
		}
	}
}

func TestOffsetGenerator_FromOffsetInvalid(t *testing.T) {
	// Invalid offset should fall back to initial
	gen := NewOffsetGeneratorFrom(Offset("invalid"))

	if got := gen.Current(); got != InitialOffset {
		t.Errorf("Current() = %q, want %q (should fall back to initial)", got, InitialOffset)
	}
}

func TestOffsetGenerator_FromOffset(t *testing.T) {
	// Start from a specific offset
	gen := NewOffsetGeneratorFrom(Offset("0000000000000000_0000000000000100"))

	if got := gen.Current(); got != Offset("0000000000000000_0000000000000100") {
		t.Errorf("Current() = %q, want %q", got, "0000000000000000_0000000000000100")
	}

	// Next offset should continue from there
	offset := gen.Next(50)
	if offset != Offset("0000000000000000_0000000000000150") {
		t.Errorf("Next(50) = %q, want %q", offset, "0000000000000000_0000000000000150")
	}
}

func TestOffsetGenerator_WithReadSeq(t *testing.T) {
	// Test with non-zero read sequence
	gen := NewOffsetGeneratorFrom(Offset("0000000000000001_0000000000000000"))

	offset := gen.Next(100)
	if offset != Offset("0000000000000001_0000000000000100") {
		t.Errorf("Next(100) = %q, want %q", offset, "0000000000000001_0000000000000100")
	}
}

func TestOffsetGenerator_ZeroBytes(t *testing.T) {
	gen := NewOffsetGenerator()

	// Appending zero bytes should not change the offset
	offset := gen.Next(0)
	if offset != InitialOffset {
		t.Errorf("Next(0) = %q, want %q", offset, InitialOffset)
	}
}

func TestParseOffset(t *testing.T) {
	tests := []struct {
		name       string
		offset     Offset
		wantSeq    uint64
		wantByte   uint64
		wantErr    bool
	}{
		{"initial", InitialOffset, 0, 0, false},
		{"simple", Offset("0000000000000000_0000000000000005"), 0, 5, false},
		{"with seq", Offset("0000000000000001_0000000000000100"), 1, 100, false},
		{"large values", Offset("0000000000001234_0000000000005678"), 1234, 5678, false},
		{"invalid format", Offset("invalid"), 0, 0, true},
		{"no underscore", Offset("12345"), 0, 0, true},
		{"empty", Offset(""), 0, 0, true},
		{"invalid seq number", Offset("abc_0000000000000000"), 0, 0, true},
		{"invalid byte number", Offset("0000000000000000_xyz"), 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seq, byteOff, err := ParseOffset(tt.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if seq != tt.wantSeq {
					t.Errorf("seq = %d, want %d", seq, tt.wantSeq)
				}
				if byteOff != tt.wantByte {
					t.Errorf("byteOffset = %d, want %d", byteOff, tt.wantByte)
				}
			}
		})
	}
}

func TestFormatOffset(t *testing.T) {
	tests := []struct {
		name       string
		seq        uint64
		byteOffset uint64
		want       Offset
	}{
		{"zero", 0, 0, InitialOffset},
		{"simple", 0, 5, Offset("0000000000000000_0000000000000005")},
		{"with seq", 1, 100, Offset("0000000000000001_0000000000000100")},
		{"large values", 1234, 5678, Offset("0000000000001234_0000000000005678")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatOffset(tt.seq, tt.byteOffset)
			if got != tt.want {
				t.Errorf("FormatOffset() = %q, want %q", got, tt.want)
			}
		})
	}
}
