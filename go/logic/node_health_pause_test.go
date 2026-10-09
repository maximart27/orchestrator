package logic

import (
	"testing"
	"time"
)

// healthFixture simulates the node health tick with a clock of its own: lastGood is the instant of
// the last heartbeat that succeeded, and the seams count the yields and the fatal errors.
type healthFixture struct {
	m                *TenantManager
	now, lastGood    time.Time
	yields, fatals   int
	heartbeatHealthy bool
}

func newHealthFixture(t *testing.T) *healthFixture {
	t.Helper()
	f := &healthFixture{m: &TenantManager{}, now: time.Unix(1_700_000_000, 0), heartbeatHealthy: true}
	f.lastGood = f.now
	f.m.healthWatch.now = func() time.Time { return f.now }
	f.m.healthWatch.sinceLastGood = func() time.Duration { return f.now.Sub(f.lastGood) }
	prevYield, prevFatal := raftYield, raftFatal
	raftYield = func() error { f.yields++; return nil }
	raftFatal = func(err error) error { f.fatals++; return err }
	t.Cleanup(func() { raftYield, raftFatal = prevYield, prevFatal })
	return f
}

// tick advances the clock and runs one health tick; the registration (another goroutine in the
// real process) records a good heartbeat before the tick only when `registrationFirst`.
func (f *healthFixture) tick(d time.Duration, registrationFirst bool) {
	f.now = f.now.Add(d)
	if registrationFirst && f.heartbeatHealthy {
		f.lastGood = f.now
	}
	f.m.checkControlHealth()
}

func (f *healthFixture) run(seconds int) {
	for i := 0; i < seconds; i++ {
		f.tick(time.Second, true)
	}
}

// A process frozen for longer than fatalAfterUnhealthyDuration wakes up with a stale heartbeat
// timestamp and its health tick may run before the heartbeat: that is the pause, not a failure.
func TestProcessPauseIsNotAHealthFailure(t *testing.T) {
	for _, pause := range []time.Duration{10 * time.Second, 50 * time.Second, 5 * time.Minute} {
		f := newHealthFixture(t)
		f.run(3)
		f.tick(pause, false) // the tick runs first after the freeze: the timestamp is stale
		if f.fatals != 0 || f.yields != 0 {
			t.Fatalf("pause %v: yields=%d fatals=%d right after waking up, want none", pause, f.yields, f.fatals)
		}
		f.lastGood = f.now // the heartbeat runs a moment later
		f.run(10)
		if f.fatals != 0 || f.yields != 0 {
			t.Errorf("pause %v: yields=%d fatals=%d after recovering, want none", pause, f.yields, f.fatals)
		}
	}
}

// The control backend failing for real keeps killing the process after fatalAfterUnhealthyDuration
// (and yielding after yieldAfterUnhealthyDuration), also when it fails right after a pause.
func TestControlBackendFailureStillYieldsAndIsFatal(t *testing.T) {
	for _, pause := range []time.Duration{0, 50 * time.Second} {
		f := newHealthFixture(t)
		f.run(3)
		if pause > 0 {
			f.tick(pause, false)
		}
		f.heartbeatHealthy = false
		seconds := int(yieldAfterUnhealthyDuration / time.Second)
		f.run(seconds - 1)
		if f.yields != 0 {
			t.Fatalf("pause %v: yielded after %ds of failure", pause, seconds-1)
		}
		f.run(2)
		if f.yields == 0 || f.fatals != 0 {
			t.Fatalf("pause %v: yields=%d fatals=%d after %ds of failure, want a yield and no fatal", pause, f.yields, f.fatals, seconds+1)
		}
		f.run(int(fatalAfterUnhealthyDuration / time.Second))
		if f.fatals == 0 {
			t.Errorf("pause %v: no fatal after %v of failure", pause, fatalAfterUnhealthyDuration)
		}
	}
}
