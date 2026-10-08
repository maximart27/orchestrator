package inst

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
)

// Producers blocked on a full write buffer must be released when the Store is
// closed, otherwise stopping a tenant leaks goroutines (and can hang).
func TestEnqueueInstanceWriteDoesNotBlockAfterClose(t *testing.T) {
	for round := 0; round < 10; round++ {
		s := newTestStore(t, fmt.Sprintf("enq-close-%d", round), func(c *config.Configuration) {
			c.InstanceWriteBufferSize = 2
			c.BufferInstanceWrites = true
			c.InstanceFlushIntervalMilliseconds = 1000
		})
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				k := InstanceKey{Hostname: fmt.Sprintf("h%d", i), Port: 3306}
				for {
					select {
					case <-stop:
						return
					default:
					}
					s.enqueueInstanceWrite(testInstance(k), true, nil)
				}
			}(i)
		}
		time.Sleep(5 * time.Millisecond)
		_ = s.Close()
		close(stop)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: producers still blocked after Close()", round)
		}
	}
}
