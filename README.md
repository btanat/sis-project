# SIS Project — Checkpoint 2 (Concurrent Processing)

Golang Application Development, KBTU, Fall 2026.

An individual, cumulative Go project built around the concept of a **task** — a
unit of work that has an input, is processed by some business logic, and
produces a result. Checkpoint 1 built a synchronous, in-memory application.
Checkpoint 2 adds **concurrent processing with a worker pool and a bounded
queue**. An HTTP interface comes in Checkpoint 3.

## 1. Purpose and processing logic

A task is processed by a **processor**. Two processors are implemented, both
satisfying a single `Processor` interface:

| Kind    | Input                         | Processing                        | Example result                      |
|---------|-------------------------------|-----------------------------------|-------------------------------------|
| `text`  | any text                      | count words / runes / letters     | `3 words, 14 runes, 12 letters`     |
| `stats` | numbers (comma- or space-sep) | compute count / min / max / avg   | `count=3 min=10 max=30 avg=20`      |

## 2. Creating and submitting tasks

```go
mgr := sis.NewManager(sis.TextAnalyzer{}, sis.StatsCalculator{})
mgr.Start(3, 10)                         // 3 workers, queue capacity 10

t, err := mgr.CreateTask("stats", "10 20 30") // validated, stored as PENDING
err = mgr.Submit(t.ID)                   // returns immediately, task is QUEUED

got, _ := mgr.GetTask(t.ID)              // safe to call at any time
mgr.Stop()                               // finish accepted tasks, stop workers
```

- `CreateTask` validates the input (non-empty, known processor kind), assigns a
  unique ID (`task-1`, `task-2`, …) and stores the task as `PENDING`.
- `Submit(id)` puts an existing `PENDING` task into the queue and **returns
  without waiting** for processing.
- `ProcessTask(id)` from Checkpoint 1 still exists and processes a `PENDING`
  task synchronously in the calling goroutine.
- `GetTask` and `Tasks` return **copies** of tasks, never pointers into the
  store (see section 8).

## 3–5. Worker pool and bounded queue

```
                     Submit(id)
                         |
                         v
           +----------------------------+
           |  queue: chan string        |   bounded: capacity = queueSize
           |  [id] [id] [id] [ ] [ ]    |
           +----------------------------+
              |          |          |
              v          v          v
          worker 1   worker 2   worker 3      fixed number of goroutines
              |          |          |
              v          v          v
           execute(id): Processor.Process(input)
              |          |          |
              v          v          v
      +----------------------------------------+
      |  tasks map[string]*Task  (guarded by   |
      |  Manager.mu, a sync.RWMutex)           |
      +----------------------------------------+
```

- **Workers:** `Start(workers, queueSize)` launches a fixed number of worker
  goroutines (the demo in `main.go` uses **3**). Each worker runs
  `for id := range queue` and keeps taking tasks until the queue is closed, so
  workers are reused instead of starting one goroutine per task.
- **Queue:** a buffered channel of task IDs, `make(chan string, queueSize)`.
  Its capacity is the queue limit (the demo uses **10**). The capacity never
  grows, and Submit never starts extra goroutines to get around it.
- Different workers process independent tasks at the same time. The order in
  which tasks finish is not guaranteed, and nothing in the code depends on it.

## 6. Full queue: reject

`Submit` uses a non-blocking send (`select` with `default`). When the queue is
full it returns **`ErrQueueFull`** and the task **stays `PENDING`**, so the
caller can try again later. A rejected task is never marked as queued or
processing. Because Submit never blocks, a caller cannot get stuck waiting for
a pool that has been stopped.

## 7. Task lifecycle

```
           Submit                worker takes it          success
PENDING ───────────▶ QUEUED ─────────────────────▶ PROCESSING ──────────▶ COMPLETED
   │                                                    │
   │  ProcessTask (synchronous, Checkpoint 1)           │  processor error
   └───────────────────────────────────────────────────▶└──────────────────▶ FAILED
```

| From         | To           | Triggered by                         |
|--------------|--------------|--------------------------------------|
| `PENDING`    | `QUEUED`     | `Submit` (task accepted into queue)  |
| `QUEUED`     | `PROCESSING` | a worker receives the task           |
| `PENDING`    | `PROCESSING` | `ProcessTask` (synchronous)          |
| `PROCESSING` | `COMPLETED`  | processor succeeded, result stored   |
| `PROCESSING` | `FAILED`     | processor returned an error          |

Everything else is rejected with `ErrInvalidTransition`: submitting a task that
is already `QUEUED`, `PROCESSING`, `COMPLETED` or `FAILED`, or calling
`ProcessTask` on a task that is not `PENDING`. Retries are not supported.
`COMPLETED` and `FAILED` are final.

The check-and-change of the state always happens inside one critical section,
so two goroutines can never both move the same task to `PROCESSING`. A task is
therefore never processed by two workers at once.

## 8. Synchronization of shared data

Shared between goroutines:

| Data                                      | Who uses it                          | Protection                    |
|-------------------------------------------|--------------------------------------|-------------------------------|
| `tasks` map and every `*Task` inside it   | callers, workers                     | `Manager.mu` (`sync.RWMutex`) |
| `nextID` counter                          | concurrent `CreateTask` calls        | `Manager.mu`                  |
| `started` / `stopped` flags               | `Start`, `Submit`, `Stop`            | `Manager.mu`                  |
| `queue` channel                           | `Submit` sends, workers receive      | the channel itself; send and close happen under `Manager.mu` |
| `processors` map                          | everyone, **read-only** after `NewManager` | none needed: concurrent reads are safe |

- **One lock for all shared state.** With a single mutex there is no lock
  ordering to get wrong, so locking cannot deadlock. Readers (`GetTask`,
  `Tasks`) take `RLock` and can run in parallel.
- **The lock is not held while a processor runs.** `execute` locks to move the
  task to `PROCESSING` and copy its input, unlocks, calls `Process`, then locks
  again to store the result. Otherwise workers would run one at a time.
- **Copies, not pointers.** Protecting the map does not protect the objects
  stored in it. If `GetTask` returned the stored `*Task`, a caller could read
  `State` while a worker writes it — a data race. So `GetTask` and `Tasks`
  return a deep copy (`snapshot`), which also copies `Result` and its `Metrics`
  map.

## 9. Stopping the workers

`Stop()`:

1. under the lock, sets `stopped = true` (from now on `Submit` returns
   `ErrPoolNotRunning`) and **closes the queue**;
2. releases the lock and waits on a `sync.WaitGroup` until every worker has
   returned.

Closing the queue does not discard anything: a `range` loop over a channel
receives every value still in the buffer and only then ends. So tasks accepted
before `Stop` are processed, and `Stop` returns only after they are finished.

**Channel ownership:** the `Manager` owns the queue. `Submit` is the only
sender and `Stop` is the only place that closes it. Both run under the same
lock, and `Submit` checks `stopped` first, so sending on a closed channel
(which would panic) cannot happen. `Stop` is safe to call more than once and
from several goroutines: only the first call closes the channel.

The `WaitGroup` is waited on **without** holding the lock, because the workers
need the lock to finish their current task.

Context-based cancellation and graceful HTTP shutdown are planned for
Checkpoint 3.

## 10. How to run

Requires **Go 1.27**.

```bash
go run .              # demo: start pool, submit tasks, stop, print states
go test ./...         # all unit tests
go test -race ./...   # the same tests with the race detector
```

Verification commands (all must be clean):

```bash
gofmt -l .            # prints nothing when formatting is correct
go vet ./...
go test ./...
go test -race ./...
```

## Tests

`manager_test.go` and `processor_test.go` are the Checkpoint 1 tests and still
pass. `pool_test.go` covers the concurrent behaviour. Concurrency tests use a
test processor whose `Process` blocks until the test releases it, so they check
real overlap without relying on `time.Sleep`.

| Test                            | What it checks                                                        |
|---------------------------------|-----------------------------------------------------------------------|
| `TestStart`                     | pool starts; invalid config and a second `Start` are rejected         |
| `TestSubmitBeforeStart`         | `Submit` before `Start` is rejected, task stays `PENDING`             |
| `TestSubmitDoesNotWait`         | `Submit` returns while the task is still `PROCESSING`                 |
| `TestProcessManyTasks`          | 30 tasks, each gets its own correct result; an invalid one is `FAILED` |
| `TestTasksOverlap`              | two tasks are inside `Process` at the same moment                     |
| `TestWorkerLimit`               | never more tasks running at once than workers                         |
| `TestSubmitInvalid`             | unknown, queued, processing and completed tasks cannot be submitted   |
| `TestQueueFull`                 | `ErrQueueFull`, task stays `PENDING`, can be resubmitted later        |
| `TestConcurrentAccess`          | 8 goroutines create, submit and read 400 tasks while workers run      |
| `TestReturnedTaskIsACopy`       | changing a returned task does not change the stored one               |
| `TestStop`                      | queued tasks are finished, workers exit, later `Submit` is rejected, second `Stop` is safe |
| `TestStopBeforeStart`           | `Stop` without `Start` neither panics nor blocks                      |
| `TestSubmitAndStopConcurrently` | `Submit` racing with two `Stop` calls: no panic, no lost or stuck tasks |

## Project structure

```
.
├── go.mod
├── main.go                     # demo of the concurrent version
└── internal/
    └── sis/
        ├── errors.go           # sentinel errors (errors.Is-friendly)
        ├── task.go             # Task, State, lifecycle methods, snapshot
        ├── processor.go        # Processor interface + TextAnalyzer, StatsCalculator
        ├── manager.go          # store, mutex, CreateTask/GetTask/ProcessTask
        ├── pool.go             # worker pool: Start, Submit, worker, Stop
        ├── processor_test.go   # processor tests (Checkpoint 1)
        ├── manager_test.go     # store and lifecycle tests (Checkpoint 1)
        └── pool_test.go        # concurrency tests (Checkpoint 2)
```
