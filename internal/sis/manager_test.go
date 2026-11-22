package sis

import (
	"errors"
	"testing"
)

func newTestManager() *Manager {
	return NewManager(TextAnalyzer{}, StatsCalculator{})
}

// Successful task creation and that it starts in the Pending state.
func TestCreateTask_Success(t *testing.T) {
	m := newTestManager()

	task, err := m.CreateTask("text", "Hello Go world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.ID == "" {
		t.Error("expected a non-empty ID")
	}
	if task.State != StatePending {
		t.Errorf("new task state = %s, want %s", task.State, StatePending)
	}
}

// Invalid input (empty/whitespace) and unknown processor kind are rejected.
func TestCreateTask_InvalidInput(t *testing.T) {
	m := newTestManager()

	if _, err := m.CreateTask("text", "   "); !errors.Is(err, ErrEmptyInput) {
		t.Errorf("empty input: expected ErrEmptyInput, got %v", err)
	}
	if _, err := m.CreateTask("nope", "data"); !errors.Is(err, ErrUnknownProcessor) {
		t.Errorf("unknown kind: expected ErrUnknownProcessor, got %v", err)
	}
}

// Retrieval of an existing task and of a nonexistent one.
func TestGetTask(t *testing.T) {
	m := newTestManager()
	created, _ := m.CreateTask("text", "abc def")

	got, err := m.GetTask(created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("got ID %s, want %s", got.ID, created.ID)
	}

	if _, err := m.GetTask("task-999"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("missing task: expected ErrTaskNotFound, got %v", err)
	}
}

// Successful processing: result is produced, attached, and state becomes Completed.
func TestProcessTask_Success(t *testing.T) {
	m := newTestManager()
	task, _ := m.CreateTask("stats", "10 20 30")

	if err := m.ProcessTask(task.ID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// CreateTask returns a copy, so read the current state from the store.
	task, _ = m.GetTask(task.ID)
	if task.State != StateCompleted {
		t.Errorf("state = %s, want %s", task.State, StateCompleted)
	}
	if task.Result == nil {
		t.Fatal("expected a result, got nil")
	}
	if task.Result.Metrics["average"] != 20 {
		t.Errorf("average = %v, want 20", task.Result.Metrics["average"])
	}
}

// Processing an already completed task is an invalid state transition.
func TestProcessTask_InvalidTransition(t *testing.T) {
	m := newTestManager()
	task, _ := m.CreateTask("text", "hello")

	if err := m.ProcessTask(task.ID); err != nil {
		t.Fatalf("first process failed: %v", err)
	}
	// Second processing attempt must be rejected.
	if err := m.ProcessTask(task.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

// A processor error moves the task to Failed and reports the error.
func TestProcessTask_ProcessorError(t *testing.T) {
	m := newTestManager()
	task, _ := m.CreateTask("stats", "1 2 notanumber")

	if err := m.ProcessTask(task.ID); !errors.Is(err, ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput, got %v", err)
	}
	task, _ = m.GetTask(task.ID)
	if task.State != StateFailed {
		t.Errorf("state = %s, want %s", task.State, StateFailed)
	}
}

// Multiple independent tasks do not mix or overwrite each other's data.
func TestMultipleIndependentTasks(t *testing.T) {
	m := newTestManager()

	t1, _ := m.CreateTask("text", "one two three")
	t2, _ := m.CreateTask("stats", "5 15")

	if err := m.ProcessTask(t1.ID); err != nil {
		t.Fatalf("process t1: %v", err)
	}
	if err := m.ProcessTask(t2.ID); err != nil {
		t.Fatalf("process t2: %v", err)
	}

	t1, _ = m.GetTask(t1.ID)
	t2, _ = m.GetTask(t2.ID)
	if t1.ID == t2.ID {
		t.Error("expected distinct IDs for distinct tasks")
	}
	if t1.Result.Metrics["words"] != 3 {
		t.Errorf("t1 words = %v, want 3", t1.Result.Metrics["words"])
	}
	if t2.Result.Metrics["average"] != 10 {
		t.Errorf("t2 average = %v, want 10", t2.Result.Metrics["average"])
	}
	if len(m.Tasks()) != 2 {
		t.Errorf("stored tasks = %d, want 2", len(m.Tasks()))
	}
}
