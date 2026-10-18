package sis

import (
	"errors"
	"testing"
)

// Table-driven test for both processors: checks successful processing, the
// correctness of produced results, and invalid-input handling in one place.
func TestProcessors(t *testing.T) {
	cases := []struct {
		name      string
		proc      Processor
		input     string
		wantErr   error              // nil means success expected
		wantMetrs map[string]float64 // checked only on success
	}{
		{
			name:      "text simple",
			proc:      TextAnalyzer{},
			input:     "Hello Go world",
			wantMetrs: map[string]float64{"words": 3, "runes": 14, "letters": 12},
		},
		{
			name:    "text empty",
			proc:    TextAnalyzer{},
			input:   "   ",
			wantErr: ErrEmptyInput,
		},
		{
			name:      "stats simple",
			proc:      StatsCalculator{},
			input:     "10, 20, 30",
			wantMetrs: map[string]float64{"count": 3, "min": 10, "max": 30, "sum": 60, "average": 20},
		},
		{
			name:      "stats single value",
			proc:      StatsCalculator{},
			input:     "42",
			wantMetrs: map[string]float64{"count": 1, "min": 42, "max": 42, "average": 42},
		},
		{
			name:    "stats non-numeric",
			proc:    StatsCalculator{},
			input:   "1 2 abc",
			wantErr: ErrInvalidInput,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.proc.Process(tc.input)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify each expected metric value.
			for k, want := range tc.wantMetrs {
				if got.Metrics[k] != want {
					t.Errorf("metric %q = %v, want %v", k, got.Metrics[k], want)
				}
			}
			if got.Summary == "" {
				t.Errorf("expected a non-empty summary")
			}
		})
	}
}
