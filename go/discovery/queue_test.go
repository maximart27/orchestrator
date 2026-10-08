package discovery

import (
	"testing"
	"time"
)

// TestQueueStopMonitoring checks that stopping a queue neither blocks forever
// (done used to be nil) nor panics when called twice.
func TestQueueStopMonitoring(t *testing.T) {
	q := CreateOrReturnQueue("stop-monitoring-test")

	finished := make(chan struct{})
	go func() {
		q.stopMonitoring()
		q.stopMonitoring()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("stopMonitoring blocked")
	}
}
