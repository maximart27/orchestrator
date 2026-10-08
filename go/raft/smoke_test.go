package orcraft_test

import (
	"testing"
	"time"
)

func TestHarnessSmoke(t *testing.T) {
	apps, fakes := newFakeApps(3, false, "a", "b")
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	for i := 0; i < 20; i++ {
		tn := []string{"a", "b"}[i%2]
		if _, err := c.Publish(ld, tn, "cat", fakeVal("k", "x")); err != nil {
			t.Fatal(err)
		}
	}
	c.WaitConverged(10 * time.Second)
	c.AssertIdentical("a", "b")
	if fakes[0].get("a", "k") != "x,x,x,x,x,x,x,x,x,x," {
		t.Fatal(fakes[0].get("a", "k"))
	}
}
