package stream

import (
	"testing"
)

func TestOffset_String(t *testing.T) {
	tests := []struct {
		name   string
		offset Offset
		want   string
	}{
		{"empty", Offset(""), ""},
		{"start", StartOffset, "-1"},
		{"now", NowOffset, "now"},
		{"regular", Offset("123_456"), "123_456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.String(); got != tt.want {
				t.Errorf("Offset.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOffset_IsStart(t *testing.T) {
	tests := []struct {
		name   string
		offset Offset
		want   bool
	}{
		{"empty string is start", Offset(""), true},
		{"StartOffset is start", StartOffset, true},
		{"explicit -1 is start", Offset("-1"), true},
		{"now is not start", NowOffset, false},
		{"regular offset is not start", Offset("123_456"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.IsStart(); got != tt.want {
				t.Errorf("Offset.IsStart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffset_IsNow(t *testing.T) {
	tests := []struct {
		name   string
		offset Offset
		want   bool
	}{
		{"NowOffset is now", NowOffset, true},
		{"explicit now string is now", Offset("now"), true},
		{"StartOffset is not now", StartOffset, false},
		{"empty is not now", Offset(""), false},
		{"regular offset is not now", Offset("123_456"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.IsNow(); got != tt.want {
				t.Errorf("Offset.IsNow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffset_LexicographicComparison(t *testing.T) {
	// Offsets must be lexicographically sortable
	tests := []struct {
		name string
		a    Offset
		b    Offset
		want int // -1 if a < b, 0 if equal, 1 if a > b
	}{
		{"equal offsets", Offset("123"), Offset("123"), 0},
		{"a before b", Offset("100"), Offset("200"), -1},
		{"a after b", Offset("200"), Offset("100"), 1},
		{"lexicographic not numeric", Offset("9"), Offset("10"), 1}, // "9" > "10" lexicographically
		{"underscore format", Offset("1_100"), Offset("1_200"), -1},
		{"underscore format equal", Offset("1_100"), Offset("1_100"), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Compare(tt.b)
			if got != tt.want {
				t.Errorf("Offset(%q).Compare(Offset(%q)) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestOffset_IsSentinel(t *testing.T) {
	tests := []struct {
		name   string
		offset Offset
		want   bool
	}{
		{"StartOffset is sentinel", StartOffset, true},
		{"NowOffset is sentinel", NowOffset, true},
		{"empty is sentinel (treated as start)", Offset(""), true},
		{"regular offset is not sentinel", Offset("123_456"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.IsSentinel(); got != tt.want {
				t.Errorf("Offset.IsSentinel() = %v, want %v", got, tt.want)
			}
		})
	}
}
