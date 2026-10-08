package discovery

import (
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
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

func newTestQueue(t *testing.T, name string) *Queue {
	t.Helper()
	q := NewQueue(name, 16)
	t.Cleanup(q.Close)
	return q
}

// TestNewQueueNotRegistered checks that NewQueue does not touch the named registry.
func TestNewQueueNotRegistered(t *testing.T) {
	q := newTestQueue(t, "unregistered-test")
	if got := ReturnQueue("unregistered-test"); got != nil {
		t.Fatalf("NewQueue must not register the queue, got %v", got)
	}
	if q.Name() != "unregistered-test" {
		t.Errorf("unexpected name %q", q.Name())
	}
	registered := CreateOrReturnQueue("registered-shim-test")
	if registered != CreateOrReturnQueue("registered-shim-test") {
		t.Errorf("named shim must return the same queue")
	}
	if registered == q {
		t.Errorf("named shim returned an unregistered queue")
	}
	registered.Close()
}

// pending returns the number of distinct keys waiting on the queue
// (QueueLen() adds channel and map sizes, which count each key twice).
func pending(q *Queue) int {
	q.Lock()
	defer q.Unlock()
	return len(q.queuedKeys)
}

// TestQueuesAreIsolated checks that two queues share neither keys, in-flight state nor length.
func TestQueuesAreIsolated(t *testing.T) {
	a := newTestQueue(t, "tenant-a")
	b := newTestQueue(t, "tenant-b")

	key := inst.InstanceKey{Hostname: "10.0.0.5", Port: 3306}

	a.Push(key)
	a.Push(key) // duplicate on the same queue is dropped
	if pending(a) != 1 {
		t.Fatalf("a: expected length 1, got %d", pending(a))
	}
	if pending(b) != 0 {
		t.Fatalf("b: expected length 0, got %d", pending(b))
	}

	// same key is accepted by the other queue: no shared dedup state
	b.Push(key)
	if pending(b) != 1 {
		t.Fatalf("b: expected length 1, got %d", pending(b))
	}

	if got := a.Consume(); got != key {
		t.Fatalf("a: unexpected key %v", got)
	}
	// while consumed in a, the key is still pushable in b's own consumed set
	if got := b.Consume(); got != key {
		t.Fatalf("b: unexpected key %v", got)
	}

	// a's key is in flight: pushing it again is a no-op until released
	a.Push(key)
	if pending(a) != 0 {
		t.Fatalf("a: in-flight key must not be re-queued, got %d", pending(a))
	}
	a.Release(key)
	a.Push(key)
	if pending(a) != 1 {
		t.Fatalf("a: released key must be re-queueable, got %d", pending(a))
	}
	// releasing in a did not release b's in-flight copy
	b.Push(key)
	if pending(b) != 0 {
		t.Fatalf("b: in-flight key must not be re-queued, got %d", pending(b))
	}
}

// TestQueueCloseIdempotent checks Close can be called repeatedly and still allows draining.
func TestQueueCloseIdempotent(t *testing.T) {
	q := NewQueue("close-test", 4)
	key := inst.InstanceKey{Hostname: "h", Port: 1}
	q.Push(key)
	q.Close()
	q.Close()
	if got := q.Consume(); got != key {
		t.Fatalf("unexpected key %v", got)
	}
}

// TestNewQueueFromConfigUsesTenantConfig checks that statistics limits come from the given config.
func TestNewQueueFromConfigUsesTenantConfig(t *testing.T) {
	cfg := &config.Configuration{DiscoveryQueueCapacity: 8, DiscoveryQueueMaxStatisticsSize: 2}
	q := NewQueueFromConfig("cfg-test", cfg)
	defer q.Close()

	if cap(q.queue) != 8 {
		t.Errorf("expected capacity 8, got %d", cap(q.queue))
	}
	for i := 0; i < 5; i++ {
		q.collectStatistics()
	}
	if got := len(q.DiscoveryQueueMetrics(100)); got != 2 {
		t.Errorf("expected statistics capped at 2 entries, got %d", got)
	}
}
