package roles

import (
	"context"
	"sync"
	"testing"
)

func TestEventOutcomesConcurrent(t *testing.T) {
	ev := NewEvent(context.Background(), map[string]interface{}{})
	var wg sync.WaitGroup
	workers := 16
	perWorker := 100
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ev.ReportOutcome(EnsureOutcome{
					Step:   "dns_forward",
					Action: EnsureActionCreate,
				})
			}
		}(w)
	}
	wg.Wait()
	outcomes := ev.Outcomes()
	if len(outcomes) != workers*perWorker {
		t.Fatalf("expected %d outcomes, got %d", workers*perWorker, len(outcomes))
	}
	// Outcomes must return a copy: mutating it must not affect the event.
	outcomes[0] = EnsureOutcome{}
	if ev.Outcomes()[0].Step != "dns_forward" {
		t.Fatal("Outcomes() did not return an isolated copy")
	}
}

func TestNewEventHasNoOutcomes(t *testing.T) {
	ev := NewEvent(context.Background(), nil)
	if len(ev.Outcomes()) != 0 {
		t.Fatal("new event should not carry outcomes")
	}
}
