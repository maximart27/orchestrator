package inst

import (
	"fmt"
	"sync"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

// Loading the hostname-resolve cache and resolving names concurrently must be race free (run with -race).
func TestHostnameResolveCacheLoadIsRaceFree(t *testing.T) {
	s := newTestStore(t, "resolve-race", func(c *config.Configuration) { c.HostnameResolveMethod = "default" })
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.LoadHostnameResolveCache() }()
		go func(i int) { defer wg.Done(); _, _ = s.ResolveHostname(fmt.Sprintf("h%d.example", i)) }(i)
	}
	wg.Wait()
}
