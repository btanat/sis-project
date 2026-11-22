// Command sis is a small demo of the SIS application (Checkpoint 2).
//
// It starts a worker pool, submits several tasks for concurrent processing,
// shows the expected error cases, stops the pool (which waits for all accepted
// tasks), and prints the final state of every task.
package main

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/btanat/sis-project/internal/sis"
)

const (
	numWorkers = 3  // number of worker goroutines in the pool
	queueSize  = 10 // capacity of the bounded task queue
)

func main() {
	// Register the available processors through the Processor interface.
	mgr := sis.NewManager(sis.TextAnalyzer{}, sis.StatsCalculator{})

	if err := mgr.Start(numWorkers, queueSize); err != nil {
		fmt.Println("start failed:", err)
		return
	}
	fmt.Printf("worker pool started: %d workers, queue capacity %d\n\n", numWorkers, queueSize)

	// Sample inputs: kind + input. The last one is invalid for the stats
	// processor, so that task will end up FAILED.
	samples := []struct {
		kind  string
		input string
	}{
		{"text", "Hello Go world"},
		{"stats", "10, 20, 30"},
		{"text", "Привет мир"}, // non-ASCII: runes differ from bytes
		{"stats", "3 1 4 1 5 9 2 6"},
		{"text", "Goroutines communicate by sharing channels"},
		{"stats", "1 2 three"},
	}

	var firstID string
	for _, s := range samples {
		t, err := mgr.CreateTask(s.kind, s.input)
		if err != nil {
			fmt.Printf("create failed (%s): %v\n", s.kind, err)
			continue
		}
		// Submit returns immediately; a worker processes the task later.
		if err := mgr.Submit(t.ID); err != nil {
			fmt.Printf("submit failed (%s): %v\n", t.ID, err)
			continue
		}
		fmt.Printf("submitted %s (%s)\n", t.ID, s.kind)
		if firstID == "" {
			firstID = t.ID
		}
	}

	// Error handling: a task that was already submitted cannot be submitted
	// again, whatever state it is in right now.
	if err := mgr.Submit(firstID); err != nil {
		fmt.Printf("\nexpected error for duplicate submit: %v\n", err)
	}
	// Validation from Checkpoint 1 still works.
	if _, err := mgr.CreateTask("text", "   "); err != nil {
		fmt.Printf("expected error for empty input: %v\n", err)
	}
	if _, err := mgr.CreateTask("image", "data"); err != nil {
		fmt.Printf("expected error for unknown kind: %v\n", err)
	}

	// Stop waits until every accepted task has been processed.
	mgr.Stop()
	fmt.Println("\nworker pool stopped")

	// After Stop, new submissions are rejected and the task stays PENDING.
	late, err := mgr.CreateTask("text", "too late")
	if err == nil {
		if err := mgr.Submit(late.ID); errors.Is(err, sis.ErrPoolNotRunning) {
			fmt.Printf("expected error after stop: %v\n", err)
		}
	}

	// Print all tasks in a stable order (by numeric ID).
	tasks := mgr.Tasks()
	sort.Slice(tasks, func(i, j int) bool { return idNumber(tasks[i].ID) < idNumber(tasks[j].ID) })

	fmt.Println("\n--- Final task states ---")
	for _, t := range tasks {
		fmt.Printf("%s input=%q", t, t.Input)
		switch {
		case t.Result != nil:
			fmt.Printf(" | %s", t.Result.Summary)
		case t.LastError != "":
			fmt.Printf(" | error: %s", t.LastError)
		}
		fmt.Println()
	}
}

// idNumber extracts N from an ID of the form "task-N", for sorting.
func idNumber(id string) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(id, "task-"))
	return n
}
