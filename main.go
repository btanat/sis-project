// Command sis is a small demo that exercises the SIS Checkpoint 1 application:
// it creates several tasks, processes them, and prints their final state.
// Checkpoint 1 has no HTTP interface yet (that is Checkpoint 3), so this is a
// plain synchronous program.
package main

import (
	"fmt"
	"sort"

	"github.com/btanat/sis-project/internal/sis"
)

func main() {
	// Register the available processors through the Processor interface.
	mgr := sis.NewManager(sis.TextAnalyzer{}, sis.StatsCalculator{})

	// A few sample inputs: kind + input.
	samples := []struct {
		kind  string
		input string
	}{
		{"text", "Hello Go world"},
		{"stats", "10, 20, 30"},
		{"text", "Привет мир"}, // non-ASCII: runes differ from bytes
		{"stats", "3 1 4 1 5 9 2 6"},
	}

	for _, s := range samples {
		t, err := mgr.CreateTask(s.kind, s.input)
		if err != nil {
			fmt.Printf("create failed (%s): %v\n", s.kind, err)
			continue
		}
		if err := mgr.ProcessTask(t.ID); err != nil {
			fmt.Printf("process failed (%s): %v\n", t.ID, err)
			continue
		}
	}

	// Demonstrate error handling: invalid input and an unknown processor.
	if _, err := mgr.CreateTask("text", "   "); err != nil {
		fmt.Printf("expected error for empty input: %v\n", err)
	}
	if _, err := mgr.CreateTask("image", "data"); err != nil {
		fmt.Printf("expected error for unknown kind: %v\n", err)
	}

	// Print all tasks in a stable order (by ID).
	tasks := mgr.Tasks()
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })

	fmt.Println("\n--- Final task states ---")
	for _, t := range tasks {
		fmt.Printf("%s input=%q -> %s", t, t.Input, t.State)
		if t.Result != nil {
			fmt.Printf(" | %s", t.Result.Summary)
		}
		fmt.Println()
	}
}
