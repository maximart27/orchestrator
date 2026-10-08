package discovery

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
)

func newSizedQueue(t *testing.T, capacity int) *Queue {
	t.Helper()
	cfg := config.Config.Clone()
	cfg.DiscoveryQueueCapacity = uint(capacity)
	q := NewQueueFromConfig("push-test", cfg)
	t.Cleanup(q.Close)
	return q
}

func key(host string) inst.InstanceKey { return inst.InstanceKey{Hostname: host, Port: 3306} }

// A Push blocked on a full channel must not hold the queue lock: Release (and
// Consume) have to keep working or the queue deadlocks.
func TestPushOnFullQueueDoesNotBlockRelease(t *testing.T) {
	q := newSizedQueue(t, 1)

	q.Push(key("a"))
	got := q.Consume() // a is being processed
	q.Push(key("b"))   // fills the channel
	pushed := make(chan struct{})
	go func() { q.Push(key("c")); close(pushed) }() // blocked on a full channel
	time.Sleep(50 * time.Millisecond)

	released := make(chan struct{})
	go func() { q.Release(got); close(released) }()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: Release blocked behind a Push stuck on a full queue")
	}
	// consuming makes room for the blocked Push
	if k := q.Consume(); k != key("b") {
		t.Fatalf("expected b, got %v", k)
	}
	select {
	case <-pushed:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Push did not complete after room was made")
	}
	if k := q.Consume(); k != key("c") {
		t.Fatalf("expected c, got %v", k)
	}
}

func TestPushContextAndCloseUnblockAndRevertReservation(t *testing.T) {
	q := newSizedQueue(t, 1)
	q.Push(key("a"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if q.PushContext(ctx, key("b")) {
		t.Fatal("PushContext on a full queue must give up when ctx is done")
	}
	// the reservation was reverted: b can be pushed again once there is room
	if k := q.Consume(); k != key("a") {
		t.Fatalf("expected a, got %v", k)
	}
	if !q.PushContext(context.Background(), key("b")) {
		t.Fatal("b should be pushable again after the failed attempt")
	}

	// Close releases a Push blocked on a full queue
	done := make(chan struct{})
	go func() { q.Push(key("c")); close(done) }()
	time.Sleep(50 * time.Millisecond)
	q.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Push still blocked after Close")
	}
	// ...and the dropped key left no reservation behind
	if got := q.QueueLen(); got != 2 { // channel(b) + queuedKeys(b): see QueueLen
		t.Fatalf("unexpected QueueLen %d", got)
	}
}

func TestPushAfterCloseWithRoomStillEnqueues(t *testing.T) {
	q := newSizedQueue(t, 4)
	q.Close()
	for i := 0; i < 100; i++ {
		if !q.PushContext(context.Background(), key(fmt.Sprintf("h%d", i))) {
			if i < 4 {
				t.Fatalf("push %d dropped although the queue has room", i)
			}
			return
		}
	}
	t.Fatal("expected pushes beyond the capacity to be dropped after Close")
}

// Many producers and slow workers on a tiny queue: no deadlock, and a key is
// never queued twice nor processed concurrently with itself.
func TestPushManyProducersSlowWorkers(t *testing.T) {
	q := newSizedQueue(t, 1)
	const producers, keys, workers = 8, 20, 3

	var mu sync.Mutex
	active := map[inst.InstanceKey]bool{}
	processed := 0
	ctx, cancel := context.WithCancel(context.Background())
	var wwg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			for {
				k, ok := q.ConsumeContext(ctx)
				if !ok {
					return
				}
				mu.Lock()
				if active[k] {
					t.Errorf("key %v processed concurrently with itself", k)
				}
				active[k] = true
				mu.Unlock()
				time.Sleep(time.Millisecond)
				mu.Lock()
				active[k] = false
				processed++
				mu.Unlock()
				q.Release(k)
			}
		}()
	}

	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := 0; i < keys; i++ {
				q.Push(key(fmt.Sprintf("h%d", i)))
			}
		}()
	}
	pushed := make(chan struct{})
	go func() { pwg.Wait(); close(pushed) }()
	select {
	case <-pushed:
	case <-time.After(20 * time.Second):
		t.Fatal("producers deadlocked")
	}
	// drain what is left, then stop the workers
	deadline := time.After(10 * time.Second)
	for q.QueueLen() > 0 {
		select {
		case <-deadline:
			t.Fatal("queue never drained")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	wwg.Wait()
	if processed < keys {
		t.Fatalf("processed %d keys, expected at least %d distinct", processed, keys)
	}
}
