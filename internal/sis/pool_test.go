package sis

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// gateProcessor lets a test control exactly when processing starts and ends.
// Process reports its input on started, then waits until release is closed.
// This makes concurrency tests deterministic without time.Sleep.
type gateProcessor struct {
	started chan string
	release chan struct{}
}

func newGate() gateProcessor {
	return gateProcessor{
		started: make(chan string, 100), // big buffer: Process never blocks on it
		release: make(chan struct{}),
	}
}

func (gateProcessor) Kind() string { return "gate" }

func (g gateProcessor) Process(input string) (Result, error) {
	g.started <- input
	<-g.release
	return Result{Summary: "processed " + input}, nil
}

// waitStarted waits until n tasks have entered Process. The timeout only
// protects the test suite from hanging forever if there is a bug.
func waitStarted(t *testing.T, g gateProcessor, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-g.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d tasks started processing", i, n)
		}
	}
}

// stopWithTimeout calls Stop and fails the test if it does not return in time
// (for example, because of a deadlock or a worker that never exits).
func stopWithTimeout(t *testing.T, m *Manager) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		m.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return: workers did not terminate")
	}
}

func mustCreate(t *testing.T, m *Manager, kind, input string) Task {
	t.Helper()
	task, err := m.CreateTask(kind, input)
	if err != nil {
		t.Fatalf("CreateTask(%q, %q): %v", kind, input, err)
	}
	return task
}

func mustState(t *testing.T, m *Manager, id string, want State) Task {
	t.Helper()
	got, err := m.GetTask(id)
	if err != nil {
		t.Fatalf("GetTask(%s): %v", id, err)
	}
	if got.State != want {
		t.Fatalf("%s: state = %s, want %s", id, got.State, want)
	}
	return got
}

// ---------------------------------------------------------------------------
// 1. Startup
// ---------------------------------------------------------------------------

func TestStart(t *testing.T) {
	m := NewManager(TextAnalyzer{})

	if err := m.Start(0, 5); !errors.Is(err, ErrInvalidPoolConfig) {
		t.Errorf("0 workers: got %v, want ErrInvalidPoolConfig", err)
	}
	if err := m.Start(2, 0); !errors.Is(err, ErrInvalidPoolConfig) {
		t.Errorf("queue size 0: got %v, want ErrInvalidPoolConfig", err)
	}
	if err := m.Start(2, 5); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Start(2, 5); !errors.Is(err, ErrPoolAlreadyStarted) {
		t.Errorf("second Start: got %v, want ErrPoolAlreadyStarted", err)
	}

	// The pool really works: a submitted task gets processed.
	task := mustCreate(t, m, "text", "one two")
	if err := m.Submit(task.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	stopWithTimeout(t, m)
	mustState(t, m, task.ID, StateCompleted)
}

func TestSubmitBeforeStart(t *testing.T) {
	m := NewManager(TextAnalyzer{})
	task := mustCreate(t, m, "text", "hello")

	if err := m.Submit(task.ID); !errors.Is(err, ErrPoolNotRunning) {
		t.Errorf("got %v, want ErrPoolNotRunning", err)
	}
	mustState(t, m, task.ID, StatePending)
}

// ---------------------------------------------------------------------------
// 2. Submit is asynchronous
// ---------------------------------------------------------------------------

// Submit must return while the task is still being processed.
func TestSubmitDoesNotWait(t *testing.T) {
	g := newGate()
	m := NewManager(g)
	if err := m.Start(2, 5); err != nil {
		t.Fatal(err)
	}

	task := mustCreate(t, m, "gate", "a")
	if err := m.Submit(task.ID); err != nil { // returns although release is closed only later
		t.Fatalf("Submit: %v", err)
	}
	waitStarted(t, g, 1)

	// The processor is blocked, so the task must still be in progress.
	mustState(t, m, task.ID, StateProcessing)

	close(g.release)
	stopWithTimeout(t, m)
	got := mustState(t, m, task.ID, StateCompleted)
	if got.Result == nil || got.Result.Summary != "processed a" {
		t.Errorf("result = %+v, want summary %q", got.Result, "processed a")
	}
}

// ---------------------------------------------------------------------------
// 3-6. Many tasks, correct results, correct states
// ---------------------------------------------------------------------------

// Every task gets its own correct result, regardless of which worker processed
// it and in which order tasks finished. One invalid task must end up Failed.
func TestProcessManyTasks(t *testing.T) {
	m := NewManager(TextAnalyzer{}, StatsCalculator{})
	if err := m.Start(3, 50); err != nil {
		t.Fatal(err)
	}

	// Task i has input "i i i", so its correct average is exactly i.
	want := make(map[string]float64)
	for i := 1; i <= 30; i++ {
		task := mustCreate(t, m, "stats", fmt.Sprintf("%d %d %d", i, i, i))
		want[task.ID] = float64(i)
		if err := m.Submit(task.ID); err != nil {
			t.Fatalf("Submit %s: %v", task.ID, err)
		}
	}
	bad := mustCreate(t, m, "stats", "1 2 notanumber")
	if err := m.Submit(bad.ID); err != nil {
		t.Fatalf("Submit bad: %v", err)
	}

	stopWithTimeout(t, m) // Stop waits until all accepted tasks are done

	for id, avg := range want {
		got := mustState(t, m, id, StateCompleted)
		if got.Result == nil || got.Result.Metrics["average"] != avg {
			t.Errorf("%s: result %+v, want average %v", id, got.Result, avg)
		}
	}

	failed := mustState(t, m, bad.ID, StateFailed)
	if failed.Result != nil {
		t.Error("failed task must not have a result")
	}
	if failed.LastError == "" {
		t.Error("failed task must have LastError set")
	}
}

// ---------------------------------------------------------------------------
// 4. Real overlap and the concurrency limit
// ---------------------------------------------------------------------------

// Two tasks are inside Process at the same moment: neither has been released,
// yet both have started. This is only possible if two workers run in parallel.
func TestTasksOverlap(t *testing.T) {
	g := newGate()
	m := NewManager(g)
	if err := m.Start(2, 5); err != nil {
		t.Fatal(err)
	}

	a := mustCreate(t, m, "gate", "a")
	b := mustCreate(t, m, "gate", "b")
	for _, id := range []string{a.ID, b.ID} {
		if err := m.Submit(id); err != nil {
			t.Fatalf("Submit %s: %v", id, err)
		}
	}

	waitStarted(t, g, 2) // both started, none released: they overlap
	mustState(t, m, a.ID, StateProcessing)
	mustState(t, m, b.ID, StateProcessing)

	close(g.release)
	stopWithTimeout(t, m)
	mustState(t, m, a.ID, StateCompleted)
	mustState(t, m, b.ID, StateCompleted)
}

// countingProcessor records the largest number of Process calls that were
// running at the same time.
type countingProcessor struct {
	mu        sync.Mutex
	active    int
	maxActive int
}

func (*countingProcessor) Kind() string { return "count" }

func (c *countingProcessor) Process(input string) (Result, error) {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()

	time.Sleep(time.Millisecond) // simulated work, so that workers overlap

	c.mu.Lock()
	c.active--
	c.mu.Unlock()
	return Result{Summary: input}, nil
}

// The pool never runs more tasks at once than it has workers.
func TestWorkerLimit(t *testing.T) {
	const workers = 3
	cp := &countingProcessor{}
	m := NewManager(cp)
	if err := m.Start(workers, 100); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 60; i++ {
		task := mustCreate(t, m, "count", fmt.Sprint(i))
		if err := m.Submit(task.ID); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	stopWithTimeout(t, m)

	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.maxActive > workers {
		t.Errorf("%d tasks ran at the same time, limit is %d workers", cp.maxActive, workers)
	}
	for _, task := range m.Tasks() {
		if task.State != StateCompleted {
			t.Errorf("%s: state %s, want COMPLETED", task.ID, task.State)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. Invalid and duplicate submissions
// ---------------------------------------------------------------------------

func TestSubmitInvalid(t *testing.T) {
	g := newGate()
	m := NewManager(g, TextAnalyzer{})
	if err := m.Start(1, 5); err != nil {
		t.Fatal(err)
	}

	// Unknown task.
	if err := m.Submit("task-999"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("unknown ID: got %v, want ErrTaskNotFound", err)
	}

	// A task that is being processed cannot be submitted again.
	busy := mustCreate(t, m, "gate", "busy")
	if err := m.Submit(busy.ID); err != nil {
		t.Fatal(err)
	}
	waitStarted(t, g, 1)
	if err := m.Submit(busy.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("processing task: got %v, want ErrInvalidTransition", err)
	}

	// A task that is waiting in the queue cannot be submitted again
	// (the only worker is busy, so this one stays queued).
	queued := mustCreate(t, m, "text", "waiting")
	if err := m.Submit(queued.ID); err != nil {
		t.Fatal(err)
	}
	mustState(t, m, queued.ID, StateQueued)
	if err := m.Submit(queued.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("queued task: got %v, want ErrInvalidTransition", err)
	}
	// A queued task also cannot be processed synchronously at the same time.
	if err := m.ProcessTask(queued.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("ProcessTask on queued task: got %v, want ErrInvalidTransition", err)
	}

	close(g.release)
	stopWithTimeout(t, m)

	// A completed task cannot be submitted again (the pool is stopped now,
	// so check the state rule on a fresh, running pool).
	m2 := NewManager(TextAnalyzer{})
	if err := m2.Start(1, 1); err != nil {
		t.Fatal(err)
	}
	done := mustCreate(t, m2, "text", "done")
	if err := m2.ProcessTask(done.ID); err != nil {
		t.Fatal(err)
	}
	if err := m2.Submit(done.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("completed task: got %v, want ErrInvalidTransition", err)
	}
	stopWithTimeout(t, m2)

	mustState(t, m, busy.ID, StateCompleted)
	mustState(t, m, queued.ID, StateCompleted)
	// The rejected duplicate submission did not start a second Process call.
	if n := len(g.started); n != 0 {
		t.Errorf("busy task was processed %d extra time(s)", n)
	}
}

// ---------------------------------------------------------------------------
// 8. Full queue
// ---------------------------------------------------------------------------

func TestQueueFull(t *testing.T) {
	g := newGate()
	m := NewManager(g)
	if err := m.Start(1, 1); err != nil { // 1 worker, room for 1 waiting task
		t.Fatal(err)
	}

	a := mustCreate(t, m, "gate", "a")
	b := mustCreate(t, m, "gate", "b")
	c := mustCreate(t, m, "gate", "c")

	if err := m.Submit(a.ID); err != nil {
		t.Fatal(err)
	}
	waitStarted(t, g, 1) // the worker took a; the queue is empty again

	if err := m.Submit(b.ID); err != nil { // fills the only slot
		t.Fatal(err)
	}
	if err := m.Submit(c.ID); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	// Rejected task keeps a valid state and is not marked as queued/processing.
	mustState(t, m, c.ID, StatePending)

	// Let a finish; the worker then takes b, and a slot frees up.
	g.release <- struct{}{} // releases exactly one Process call (a)
	waitStarted(t, g, 1)    // b has started
	if err := m.Submit(c.ID); err != nil {
		t.Fatalf("resubmit after space freed: %v", err)
	}

	close(g.release)
	stopWithTimeout(t, m)
	for _, id := range []string{a.ID, b.ID, c.ID} {
		mustState(t, m, id, StateCompleted)
	}
}

// ---------------------------------------------------------------------------
// 9. Safe concurrent access to shared data
// ---------------------------------------------------------------------------

// Many goroutines create, submit and read tasks at the same time while workers
// update them. Run with -race. Every task must get exactly its own result.
func TestConcurrentAccess(t *testing.T) {
	m := NewManager(StatsCalculator{})
	if err := m.Start(4, 500); err != nil {
		t.Fatal(err)
	}

	type created struct {
		id  string
		avg float64
	}
	results := make(chan created, 400)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				v := g*100 + i
				task, err := m.CreateTask("stats", fmt.Sprintf("%d %d", v, v))
				if err != nil {
					t.Errorf("CreateTask: %v", err)
					return
				}
				if err := m.Submit(task.ID); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
				results <- created{task.ID, float64(v)}

				// Read while workers are writing.
				if _, err := m.GetTask(task.ID); err != nil {
					t.Errorf("GetTask: %v", err)
				}
				_ = m.Tasks()
			}
		}(g)
	}
	wg.Wait()
	stopWithTimeout(t, m)
	close(results)

	seen := make(map[string]bool)
	for r := range results {
		if seen[r.id] {
			t.Fatalf("duplicate task ID %s", r.id)
		}
		seen[r.id] = true
		got := mustState(t, m, r.id, StateCompleted)
		if got.Result.Metrics["average"] != r.avg {
			t.Errorf("%s: average %v, want %v", r.id, got.Result.Metrics["average"], r.avg)
		}
	}
	if len(seen) != 400 {
		t.Errorf("tasks = %d, want 400", len(seen))
	}
}

// A copy returned by GetTask is independent from the stored task: changing it
// does not change the Manager's data.
func TestReturnedTaskIsACopy(t *testing.T) {
	m := NewManager(StatsCalculator{})
	task := mustCreate(t, m, "stats", "10 20")
	if err := m.ProcessTask(task.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := m.GetTask(task.ID)
	got.State = StateFailed
	got.Result.Metrics["average"] = -1

	again := mustState(t, m, task.ID, StateCompleted)
	if again.Result.Metrics["average"] != 15 {
		t.Errorf("stored result was modified through a copy: %v", again.Result.Metrics["average"])
	}
}

// ---------------------------------------------------------------------------
// 10-11. Shutdown
// ---------------------------------------------------------------------------

// Stop waits for the workers, does not lose accepted tasks, rejects new
// submissions, and can be called twice.
func TestStop(t *testing.T) {
	g := newGate()
	m := NewManager(g)
	if err := m.Start(2, 10); err != nil {
		t.Fatal(err)
	}

	var ids []string
	for i := 0; i < 5; i++ {
		task := mustCreate(t, m, "gate", fmt.Sprint(i))
		if err := m.Submit(task.ID); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, task.ID)
	}
	waitStarted(t, g, 2) // 2 tasks processing, 3 still waiting in the queue

	// Stop is called while 3 accepted tasks are still in the queue. It must
	// not drop them: the workers drain the queue before they exit.
	stopped := make(chan struct{})
	go func() {
		m.Stop()
		close(stopped)
	}()
	close(g.release) // let the workers finish
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return: workers did not terminate")
	}

	// Stop has returned, so every accepted task must already be finished.
	for _, id := range ids {
		mustState(t, m, id, StateCompleted)
	}

	// After shutdown new submissions are rejected and the task stays Pending.
	late := mustCreate(t, m, "gate", "late")
	if err := m.Submit(late.ID); !errors.Is(err, ErrPoolNotRunning) {
		t.Errorf("Submit after Stop: got %v, want ErrPoolNotRunning", err)
	}
	mustState(t, m, late.ID, StatePending)

	// A second Stop is a safe no-op.
	stopWithTimeout(t, m)
}

func TestStopBeforeStart(t *testing.T) {
	m := NewManager(TextAnalyzer{})
	stopWithTimeout(t, m) // must not panic or block
	stopWithTimeout(t, m)
}

// Submit and Stop race on purpose. There must be no panic (send on a closed
// channel), and every submission must end in a consistent state: accepted
// tasks are Completed, rejected ones are still Pending.
func TestSubmitAndStopConcurrently(t *testing.T) {
	m := NewManager(StatsCalculator{})
	if err := m.Start(3, 20); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		id  string
		err error
	}
	outcomes := make(chan outcome, 200)

	// Stop starts once some submissions have gone through, so the two really
	// overlap instead of Stop winning before anything is submitted.
	startStop := make(chan struct{})
	var once sync.Once

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if i == 10 {
					once.Do(func() { close(startStop) })
				}
				task, err := m.CreateTask("stats", "1 2 3")
				if err != nil {
					t.Errorf("CreateTask: %v", err)
					return
				}
				outcomes <- outcome{task.ID, m.Submit(task.ID)}
			}
		}()
	}
	// Two concurrent Stop calls while submissions are in flight.
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startStop
			m.Stop()
		}()
	}

	wg.Wait()
	close(outcomes)

	accepted := 0
	for o := range outcomes {
		switch {
		case o.err == nil:
			accepted++
			got := mustState(t, m, o.id, StateCompleted)
			if got.Result.Metrics["average"] != 2 {
				t.Errorf("%s: wrong result %v", o.id, got.Result.Metrics["average"])
			}
		case errors.Is(o.err, ErrQueueFull), errors.Is(o.err, ErrPoolNotRunning):
			mustState(t, m, o.id, StatePending)
		default:
			t.Errorf("%s: unexpected error %v", o.id, o.err)
		}
	}
	t.Logf("accepted before shutdown: %d of 200", accepted)
}
