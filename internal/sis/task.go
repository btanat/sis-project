package sis

import (
	"fmt"
	"maps"
)

// State represents the lifecycle stage of a task. It is a named string type so
// that the allowed values are self-documenting and can be printed directly.
type State string

const (
	// StatePending: the task has been created but not yet submitted or processed.
	StatePending State = "PENDING"
	// StateQueued: the task was accepted by Submit and is waiting in the queue.
	StateQueued State = "QUEUED"
	// StateProcessing: a worker (or ProcessTask) is currently processing the task.
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
//
// Since Checkpoint 2 the Manager never hands out a *Task from its store. Callers
// always receive a copy (see snapshot), so a worker can update the stored task
// while someone else reads their copy, without a data race.
type Task struct {
	ID        string
	Kind      string // which processor handles this task
	Input     string
	State     State
	Result    *Result
	LastError string // human-readable reason if the task ended in StateFailed
}

// String gives a compact representation, handy for logging and the demo in main.
// It uses a value receiver because callers now hold Task values (copies), and
// fmt only finds a pointer-receiver String method on a *Task.
func (t Task) String() string {
	return fmt.Sprintf("Task{ID:%s, Kind:%s, State:%s}", t.ID, t.Kind, t.State)
}

// snapshot returns an independent copy of the task. Copying the struct alone is
// not enough: Result is a pointer and Metrics is a map, so a shallow copy would
// still share them with the stored task. We copy both, so the caller can never
// read or modify memory that belongs to the Manager's store.
// The caller must hold the Manager's lock while calling snapshot.
func (t *Task) snapshot() Task {
	cp := *t
	if t.Result != nil {
		r := *t.Result
		r.Metrics = maps.Clone(t.Result.Metrics)
		cp.Result = &r
	}
	return cp
}

// markQueued moves a task from Pending to Queued when it is accepted by Submit.
func (t *Task) markQueued() error {
	if t.State != StatePending {
		return fmt.Errorf("%w: cannot submit a task in state %s", ErrInvalidTransition, t.State)
	}
	t.State = StateQueued
	return nil
}

// markProcessing moves a task to Processing. The expected source state is a
// parameter because there are two ways to start processing:
//   - ProcessTask (synchronous, from Checkpoint 1) starts from Pending;
//   - a worker starts from Queued.
//
// Anything else is an invalid transition. Because this check-and-change runs
// under the Manager's lock, two goroutines can never both move the same task to
// Processing.
func (t *Task) markProcessing(from State) error {
	if t.State != from {
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
