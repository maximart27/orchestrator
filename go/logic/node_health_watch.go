package logic

import (
	"sync"
	"time"

	"github.com/proxysql/orchestrator/go/process"
)

// healthPauseGap is the gap between two health ticks (they are HealthPollSeconds apart) above
// which the process itself is taken to have been paused (SIGSTOP, docker pause, a frozen VM):
// nothing ran, so nothing could register the node health either.
const healthPauseGap = yieldAfterUnhealthyDuration

// controlHealthWatch tells for how long the control backend has been failing to register the
// node health, as the yield and fatal decisions of the health tick need it.
//
// The heartbeat timestamp is a wall-clock instant: a process that was frozen for longer than
// fatalAfterUnhealthyDuration wakes up with a stale one, and the health tick may run before the
// heartbeat does. That staleness is the pause, not a failure of the backend, so a pause restarts
// the count from the instant the process woke up. A backend that really fails keeps the node
// unhealthy for fatalAfterUnhealthyDuration of running time as before.
type controlHealthWatch struct {
	// now and sinceLastGood are seams for tests (nil: the real ones).
	now           func() time.Time
	sinceLastGood func() time.Duration

	mu       sync.Mutex
	lastTick time.Time
	resumeAt time.Time
}

func (w *controlHealthWatch) unhealthyFor() time.Duration {
	now, sinceGood := time.Now(), process.SinceLastGoodHealthCheck
	if w.now != nil {
		now = w.now()
	}
	if w.sinceLastGood != nil {
		sinceGood = w.sinceLastGood
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.lastTick.IsZero() && now.Sub(w.lastTick) > healthPauseGap {
		w.resumeAt = now
	}
	w.lastTick = now
	d := sinceGood()
	if !w.resumeAt.IsZero() {
		if sinceResume := now.Sub(w.resumeAt); sinceResume < d {
			d = sinceResume
		}
	}
	return d
}
