# SIS Project — Checkpoint 1 (Core Go)

Golang Application Development, KBTU, Fall 2026.

An individual, cumulative Go project built around the concept of a **task** — a
unit of work that has an input, is processed by some business logic, and
produces a result. Checkpoint 1 is a small **synchronous, in-memory**
application. Concurrency (Checkpoint 2) and an HTTP interface (Checkpoint 3) are
intentionally not implemented yet, but the code is structured so they can be
added later.

## Domain

A task is processed by a **processor**. Two processors are implemented, both
satisfying a single `Processor` interface:

| Kind    | Input                         | Processing                        | Example result                      |
|---------|-------------------------------|-----------------------------------|-------------------------------------|
| `text`  | any text                      | count words / runes / letters     | `3 words, 14 runes, 12 letters`     |
| `stats` | numbers (comma- or space-sep) | compute count / min / max / avg   | `count=3 min=10 max=30 avg=20`      |

## Task lifecycle

```
            ProcessTask (success)
PENDING ───────────────────────────▶ PROCESSING ──────▶ COMPLETED
   │                                                (result attached)
   │          ProcessTask (processor returns error)
   └──────────────────────────────────────────────────▶ FAILED
```

Allowed transitions:

- A new task starts in **PENDING**.
- `ProcessTask` moves **PENDING → PROCESSING**, then to **COMPLETED** (on
  success, with the result stored) or **FAILED** (if the processor returns an
  error).
- Processing a task that is **not** PENDING (e.g. already COMPLETED) is rejected
  with `ErrInvalidTransition`.

## Project structure

```
.
├── go.mod
├── main.go                     # demo: creates and processes sample tasks
└── internal/
    └── sis/
        ├── errors.go           # sentinel errors (errors.Is-friendly)
        ├── task.go             # Task struct, State, lifecycle methods
        ├── processor.go        # Processor interface + TextAnalyzer, StatsCalculator
        ├── manager.go          # in-memory store + lifecycle enforcement
        ├── processor_test.go   # table-driven processor tests
        └── manager_test.go     # creation, retrieval, processing, transitions
```

## How to run

Requires **Go 1.27**.

```bash
go run .          # run the demo program
go test ./...     # run all unit tests
```

Verification commands (all must be clean):

```bash
gofmt -l .        # prints nothing when formatting is correct
go vet ./...
go test ./...
```

## Design notes (for the defense)

- **Structs & methods:** `Task` holds the domain data; its lifecycle methods
  (`markProcessing`, `markCompleted`, `markFailed`) use **pointer receivers**
  because they mutate the task in place.
- **Interface:** `Processor` decouples the `Manager` from concrete processing
  logic. The manager depends only on the interface, so new processors can be
  added in later checkpoints without touching storage or lifecycle code.
  `TextAnalyzer` and `StatsCalculator` are empty structs with **value
  receivers**, since they carry no state.
- **Slices/maps:** tasks are stored in a `map[string]*Task` for O(1) lookup by
  ID; processors are registered in a `map[string]Processor` by their `Kind()`.
- **Error handling:** all expected failures are returned as errors (never
  `panic`). Sentinel errors are wrapped with `%w` so callers and tests can use
  `errors.Is`.
- **Value vs pointer semantics:** `Task.Result` is a `*Result`, so "not
  processed yet" is represented by `nil` rather than an empty value.
