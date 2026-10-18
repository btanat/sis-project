package sis

import "fmt"

// State represents the lifecycle stage of a task. It is a named string type so
// that the allowed values are self-documenting and can be printed directly.
type State string

const (
	// StatePending: the task has been created but not yet processed.
	StatePending State = "PENDING"
	// StateProcessing: the task is currently being processed.
	StateProcessing State = "PROCESSING"
	// StateCompleted: the task was processed successfully and has a result.
	StateCompleted State = "COMPLETED"
	// StateFailed: processing was attempted but the processor returned an error.
	StateFailed State = "FAILED"
)

// Result holds the output produced by processing a task.
//
// Metrics is a map because different processors produce different named values
// (the text analyzer produces "words"/"characters", the statistics processor
// produces "min"/"max"/"average"). Using float64 lets the same type carry both
// integer-like counts and real averages. Summary is a short human-readable line.
type Result struct {
	Metrics map[string]float64
	Summary string
}

// Task is the central domain model. Each task owns its input, tracks its current
// state, and (once processed) holds a Result.
//
// Result is a pointer so that "no result yet" is represented by nil, which is
// clearer than an empty Result value.
type Task struct {
	ID        string
	Kind      string // which processor handles this task
	Input     string
	State     State
	Result    *Result
	LastError string // human-readable reason if the task ended in StateFailed
}

// String gives a compact representation, handy for logging and the demo in main.
func (t *Task) String() string {
	return fmt.Sprintf("Task{ID:%s, Kind:%s, State:%s}", t.ID, t.Kind, t.State)
}

// markProcessing moves a task from Pending to Processing. It is a pointer
// receiver because it mutates the task in place. A task may only start
// processing from the Pending state; anything else is an invalid transition.
func (t *Task) markProcessing() error {
	if t.State != StatePending {
		return fmt.Errorf("%w: cannot process a task in state %s", ErrInvalidTransition, t.State)
	}
	t.State = StateProcessing
	return nil
}

// markCompleted stores the result and moves the task to Completed.
func (t *Task) markCompleted(r Result) {
	t.Result = &r
	t.State = StateCompleted
	t.LastError = ""
}

// markFailed records the failure reason and moves the task to Failed.
func (t *Task) markFailed(err error) {
	t.State = StateFailed
	t.LastError = err.Error()
}
