package sis

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Processor is the core interface of the application. Any type that knows how to
// turn a raw string input into a Result can be a Processor. The Manager depends
// only on this interface, so new kinds of task processing can be added in later
// checkpoints without changing the storage or lifecycle code.
type Processor interface {
	// Kind returns the stable name of this processor (used as the task's Kind).
	Kind() string
	// Process validates and transforms the input into a Result. Domain-level
	// problems with the input must be returned as an error, never panicked.
	Process(input string) (Result, error)
}

// --- Text analyzer ---------------------------------------------------------

// TextAnalyzer counts words, characters and runes in a piece of text.
// It is an empty struct because it has no state of its own; it only needs the
// input passed to Process. Methods use a value receiver for that reason.
type TextAnalyzer struct{}

func (TextAnalyzer) Kind() string { return "text" }

func (TextAnalyzer) Process(input string) (Result, error) {
	// The Manager already rejects empty input, but a processor should also be
	// safe on its own, so we re-check here.
	if strings.TrimSpace(input) == "" {
		return Result{}, ErrEmptyInput
	}

	words := len(strings.Fields(input)) // Fields splits on any run of whitespace
	runes := len([]rune(input))         // number of Unicode code points
	bytes := len(input)                 // number of bytes (differs for non-ASCII)

	letters := 0
	for _, r := range input {
		if unicode.IsLetter(r) {
			letters++
		}
	}

	res := Result{
		Metrics: map[string]float64{
			"words":   float64(words),
			"runes":   float64(runes),
			"bytes":   float64(bytes),
			"letters": float64(letters),
		},
		Summary: fmt.Sprintf("%d words, %d runes, %d letters", words, runes, letters),
	}
	return res, nil
}

// --- Statistics calculator -------------------------------------------------

// StatsCalculator parses a list of numbers (comma- or space-separated) and
// computes basic statistics. It also has no state, so it is an empty struct.
type StatsCalculator struct{}

func (StatsCalculator) Kind() string { return "stats" }

func (StatsCalculator) Process(input string) (Result, error) {
	if strings.TrimSpace(input) == "" {
		return Result{}, ErrEmptyInput
	}

	// Allow both commas and spaces as separators: replace commas with spaces,
	// then split on whitespace.
	fields := strings.Fields(strings.ReplaceAll(input, ",", " "))
	if len(fields) == 0 {
		return Result{}, ErrEmptyInput
	}

	values := make([]float64, 0, len(fields))
	for _, f := range fields {
		v, err := strconv.ParseFloat(f, 64)
		if err != nil {
			// Wrap the sentinel so callers can detect it with errors.Is while
			// still seeing which token failed.
			return Result{}, fmt.Errorf("%w: %q is not a number", ErrInvalidInput, f)
		}
		values = append(values, v)
	}

	min, max, sum := values[0], values[0], 0.0
	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += v
	}
	avg := sum / float64(len(values))

	res := Result{
		Metrics: map[string]float64{
			"count":   float64(len(values)),
			"min":     min,
			"max":     max,
			"sum":     sum,
			"average": avg,
		},
		Summary: fmt.Sprintf("count=%d min=%g max=%g avg=%g", len(values), min, max, avg),
	}
	return res, nil
}
