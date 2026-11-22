package sis

import (
	"fmt"
	"sync"
)

// Manager is the application service. It owns the in-memory store of tasks and
// the set of available processors, enforces the task lifecycle, and (since
// Checkpoint 2) runs the worker pool that processes tasks concurrently.
//
// Synchronization: a single RWMutex (mu) protects everything that can change
// while goroutines are running:
//   - the tasks map and every *Task stored in it (state, result, error);
//   - the nextID counter;
//   - the pool flags (started, stopped) and the act of sending to / closing queue.
//
// Using one lock for all of this means there is no lock ordering to get wrong,
// so the locks themselves cannot deadlock. RWMutex lets many readers (GetTask,
// Tasks) run at the same time, while writers take the exclusive lock.
//
// The lock is never held while a processor runs (see execute), so workers can
// process different tasks in parallel.
//
// The processors map is written only in NewManager, before any goroutine
// starts, and is read-only afterwards. Concurrent reads of a map are safe, so it
// does not need the lock.
type Manager struct {
	mu         sync.RWMutex
	tasks      map[string]*Task
	processors map[string]Processor
	nextID     int

	// Worker pool (see pool.go).
	queue   chan string    // bounded queue of task IDs; capacity is set in Start
	wg      sync.WaitGroup // counts running workers; Stop waits for it
	started bool           // Start has been called
	stopped bool           // Stop has been called; no new submissions
}

// NewManager creates an empty Manager and registers the given processors by
// their Kind(). Registering through the Processor interface (not concrete types)
// is what makes the design extensible.
func NewManager(procs ...Processor) *Manager {
	m := &Manager{
		tasks:      make(map[string]*Task),
		processors: make(map[string]Processor),
	}
	for _, p := range procs {
		m.processors[p.Kind()] = p
	}
	return m
}

// CreateTask validates the input, assigns a unique ID, stores the task in the
// Pending state, and returns a copy of it. It rejects empty input and unknown
// processor kinds using normal error handling.
func (m *Manager) CreateTask(kind, input string) (Task, error) {
	if !isMeaningful(input) {
		return Task{}, ErrEmptyInput
	}
	if _, ok := m.processors[kind]; !ok { // read-only map, no lock needed
		return Task{}, fmt.Errorf("%w: %q", ErrUnknownProcessor, kind)
	}

	// nextID++ is a read-modify-write: without the lock two goroutines could
	// read the same value and create two tasks with the same ID.
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	t := &Task{
		ID:    fmt.Sprintf("task-%d", m.nextID),
		Kind:  kind,
		Input: input,
		State: StatePending,
	}
	m.tasks[t.ID] = t
	return t.snapshot(), nil
}

// GetTask returns a copy of the task with the given ID, or ErrTaskNotFound.
// It is safe to call while workers are processing tasks.
func (m *Manager) GetTask(id string) (Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, ok := m.tasks[id]
	if !ok {
		return Task{}, fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	return t.snapshot(), nil
}

// Tasks returns copies of all stored tasks. The order is not guaranteed because
// it comes from a map; callers that need a stable order should sort by ID.
func (m *Manager) Tasks() []Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t.snapshot())
	}
	return out
}

// ProcessTask processes a Pending task synchronously in the calling goroutine,
// exactly as in Checkpoint 1. It is kept so the Checkpoint 1 API still works;
// asynchronous processing goes through Submit and the worker pool.
//
// On a processor error the task is moved to Failed and the error is returned;
// on success the task is moved to Completed with its Result attached.
func (m *Manager) ProcessTask(id string) error {
	return m.execute(id, StatePending)
}

// execute runs one task through its processor. It is shared by ProcessTask
// (from = Pending) and the workers (from = Queued). It has three phases:
//
//  1. Under the lock: find the task, move it to Processing, and copy what the
//     processor needs (processor, input) into local variables.
//  2. Without the lock: run the processor. This is the slow part; holding the
//     lock here would make all workers wait for each other, and GetTask would
//     block until processing finished.
//  3. Under the lock again: store the result (Completed) or the error (Failed).
//
// Because phase 1 checks and changes the state in one critical section, two
// goroutines can never both start processing the same task.
func (m *Manager) execute(id string, from State) error {
	// Phase 1.
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	if err := t.markProcessing(from); err != nil {
		m.mu.Unlock()
		return err
	}
	proc := m.processors[t.Kind] // guaranteed to exist: checked at creation
	input := t.Input
	m.mu.Unlock()

	// Phase 2: no lock held.
	result, err := proc.Process(input)

	// Phase 3.
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		t.markFailed(err)
		return err
	}
	t.markCompleted(result)
	return nil
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
