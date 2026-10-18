package sis

import "fmt"

// Manager is the application service. It owns the in-memory store of tasks and
// the set of available processors, and it enforces the task lifecycle.
//
// The store is a map keyed by task ID for O(1) lookup. nextID is a simple
// counter used to generate unique identifiers. Concurrency is intentionally NOT
// handled here — that is the subject of Checkpoint 2.
type Manager struct {
	tasks      map[string]*Task
	processors map[string]Processor
	nextID     int
}

// NewManager creates an empty Manager and registers the given processors by
// their Kind(). Registering through the Processor interface (not concrete types)
// is what makes the design extensible.
func NewManager(procs ...Processor) *Manager {
	m := &Manager{
		tasks:      make(map[string]*Task),
		processors: make(map[string]Processor),
		nextID:     0,
	}
	for _, p := range procs {
		m.processors[p.Kind()] = p
	}
	return m
}

// CreateTask validates the input, assigns a unique ID, stores the task in the
// Pending state, and returns it. It rejects empty input and unknown processor
// kinds using normal error handling.
func (m *Manager) CreateTask(kind, input string) (*Task, error) {
	if !isMeaningful(input) {
		return nil, ErrEmptyInput
	}
	if _, ok := m.processors[kind]; !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownProcessor, kind)
	}

	m.nextID++
	id := fmt.Sprintf("task-%d", m.nextID)

	t := &Task{
		ID:    id,
		Kind:  kind,
		Input: input,
		State: StatePending,
	}
	m.tasks[id] = t
	return t, nil
}

// GetTask returns the task with the given ID, or ErrTaskNotFound.
func (m *Manager) GetTask(id string) (*Task, error) {
	t, ok := m.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	return t, nil
}

// ProcessTask runs the processor associated with the task's kind and updates the
// task's state and result. It only processes tasks that are currently Pending;
// processing a task in any other state is an invalid transition.
//
// On a processor error the task is moved to Failed and the error is returned;
// on success the task is moved to Completed with its Result attached.
func (m *Manager) ProcessTask(id string) error {
	t, err := m.GetTask(id)
	if err != nil {
		return err
	}

	// Enforce the lifecycle: Pending -> Processing.
	if err := t.markProcessing(); err != nil {
		return err
	}

	proc := m.processors[t.Kind] // guaranteed to exist: checked at creation
	result, err := proc.Process(t.Input)
	if err != nil {
		t.markFailed(err)
		return err
	}

	t.markCompleted(result)
	return nil
}

// Tasks returns all stored tasks as a slice. The order is not guaranteed because
// it comes from a map; callers that need a stable order should sort by ID.
func (m *Manager) Tasks() []*Task {
	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out
}

// isMeaningful reports whether the input contains at least one non-whitespace
// character.
func isMeaningful(s string) bool {
	for _, r := range s {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return true
		}
	}
	return false
}
