package inst

import (
	"testing"
)

func TestApplyPoolInstancesWritesOnSQLite(t *testing.T) {
	s := newTestStore(t, "pool-sqlite", nil)
	if err := s.ApplyPoolInstances(NewPoolInstancesSubmission("pool1", "db1:3306,db2:3306")); err != nil {
		t.Fatal(err)
	}
	var n int
	dbh, err := s.backend.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := dbh.QueryRow(`select count(*) from database_instance_pool where pool = 'pool1'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 pool rows, got %d", n)
	}
	// re-applying replaces, not appends
	if err := s.ApplyPoolInstances(NewPoolInstancesSubmission("pool1", "db1:3306")); err != nil {
		t.Fatal(err)
	}
	if err := dbh.QueryRow(`select count(*) from database_instance_pool where pool = 'pool1'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 pool row after replace, got %d", n)
	}
}

// A backend that dies after its handle was cached must make the write return an
// error, not panic (a broken tenant must not take the process down).
func TestApplyPoolInstancesBackendFailureReturnsError(t *testing.T) {
	s := newTestStore(t, "pool-broken", nil)
	dbh, err := s.backend.Open()
	if err != nil {
		t.Fatal(err)
	}
	_ = dbh.Close() // Begin() on the cached handle now fails

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ApplyPoolInstances panicked: %v", r)
		}
	}()
	if err := s.ApplyPoolInstances(NewPoolInstancesSubmission("pool1", "db1:3306")); err == nil {
		t.Fatal("expected an error from a broken backend")
	}
	// the write slot must have been released
	if err := s.ExecDBWriteFunc(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}
