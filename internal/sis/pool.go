package sis

import "fmt"

// Worker pool: a fixed number of worker goroutines that receive task IDs from a
// bounded, channel-based queue.
//
//	Submit(id) --> [ queue: chan string, capacity N ] --> worker 1 --> execute
//	                                                  --> worker 2 --> execute
//	                                                  --> worker 3 --> execute
//
// Channel ownership: the Manager owns m.queue. Submit is the only sender and
// Stop is the only place that closes it. Both run under m.mu, and Submit checks
// m.stopped first, so a send on a closed channel (which panics) is impossible.
// Workers only receive.

// Start launches the worker pool with the given number of workers and the given
// queue capacity. It returns immediately; the workers wait for tasks in the
// background. A pool can be started only once.
func (m *Manager) Start(workers, queueSize int) error {
	if workers < 1 || queueSize < 1 {
		return ErrInvalidPoolConfig
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return ErrPoolAlreadyStarted
	}
	m.started = true
	m.queue = make(chan string, queueSize) // buffered = bounded queue

	for i := 0; i < workers; i++ {
		m.wg.Add(1) // before "go", so Stop can never see a zero counter too early
		go m.worker()
	}
	return nil
}

// worker processes tasks until the queue is closed and empty. "range" over a
// channel keeps receiving until the channel is closed AND every buffered value
// has been received, so tasks that were accepted before Stop are still
// processed.
func (m *Manager) worker() {
	defer m.wg.Done()
	for id := range m.queue {
		// The outcome (Completed or Failed, with the error text) is stored in
		// the task itself, so the worker has nothing to return to.
		_ = m.execute(id, StateQueued)
	}
}

// Submit puts a Pending task into the queue for asynchronous processing and
// returns immediately; it never waits for the task to be processed.
//
// Full queue policy: REJECT. If the queue has no free slot, Submit returns
// ErrQueueFull and the task stays Pending, so the caller can try again later.
// We never block, so a caller cannot get stuck waiting for a pool that has been
// stopped.
//
// Errors:
//   - ErrPoolNotRunning: Start was not called yet, or Stop was already called;
//   - ErrTaskNotFound: no task with this ID;
//   - ErrInvalidTransition: the task is not Pending (already queued, being
//     processed, completed or failed);
//   - ErrQueueFull: the queue is full.
func (m *Manager) Submit(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started || m.stopped {
		return ErrPoolNotRunning
	}
	t, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	if t.State != StatePending {
		return fmt.Errorf("%w: cannot submit a task in state %s", ErrInvalidTransition, t.State)
	}

	// Non-blocking send: "default" runs when the buffer is full. This matters
	// because we hold m.mu here. A blocking send under the lock would deadlock:
	// the workers need m.mu to finish their current task and free a slot.
	select {
	case m.queue <- id:
		// The state changes in the same critical section as the send, so no
		// one can observe a task that is in the queue but still Pending.
		return t.markQueued()
	default:
		return ErrQueueFull
	}
}

// Stop shuts the pool down gracefully:
//  1. stop accepting new tasks (stopped = true, Submit returns ErrPoolNotRunning);
//  2. close the queue, so each worker's range loop ends after the queue drains;
//  3. wait until every worker has finished.
//
// Tasks that were accepted before Stop are processed, not discarded. Stop is
// safe to call more than once and from several goroutines: only the first call
// closes the channel, later calls just wait for the workers. Calling Stop
// before Start does nothing.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.started && !m.stopped {
		m.stopped = true
		close(m.queue)
	}
	m.mu.Unlock()

	// Wait WITHOUT holding the lock: workers need m.mu to finish their tasks.
	// Waiting while holding it would deadlock.
	m.wg.Wait()
}
