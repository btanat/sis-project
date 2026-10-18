package sis

import "errors"

// Sentinel errors returned by the application. They are declared as package-level
// variables so that callers (and tests) can compare against them with errors.Is,
// instead of matching on error strings.
var (
	// ErrEmptyInput is returned when a task is created with input that is
	// empty or contains only whitespace.
	ErrEmptyInput = errors.New("sis: task input is empty")

	// ErrUnknownProcessor is returned when a task is created for a processor
	// kind that has not been registered.
	ErrUnknownProcessor = errors.New("sis: unknown processor kind")

	// ErrTaskNotFound is returned when a task with the given ID does not exist.
	ErrTaskNotFound = errors.New("sis: task not found")

	// ErrInvalidTransition is returned when an operation is attempted on a task
	// whose current state does not allow it (for example, processing a task
	// that is already completed).
	ErrInvalidTransition = errors.New("sis: invalid state transition")

	// ErrInvalidInput is returned by a processor when the input is syntactically
	// present but not valid for that specific domain (for example, the
	// statistics processor receiving non-numeric values).
	ErrInvalidInput = errors.New("sis: invalid input for processor")
)
