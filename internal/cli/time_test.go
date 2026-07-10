package cli

import (
	"testing"
	"time"
)

func TestParseTimeAcceptsDateAndRFC3339(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  time.Time
	}{
		{"2026-07-10", time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)},
		{"2026-07-10T12:30:45+08:00", time.Date(2026, 7, 10, 4, 30, 45, 0, time.UTC)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.input, func(t *testing.T) {
			t.Parallel()
			got, err := parseTime(test.input)
			if err != nil {
				t.Fatalf("parseTime() error = %v", err)
			}
			if !got.Equal(test.want) || got.Location() != time.UTC {
				t.Fatalf("parseTime() = %s (%s), want %s UTC", got, got.Location(), test.want)
			}
		})
	}
}

func TestParseTimeRejectsAmbiguousValue(t *testing.T) {
	t.Parallel()
	if _, err := parseTime("07/10/2026"); err == nil {
		t.Fatal("parseTime() expected error")
	}
}
