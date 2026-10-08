package inst

import (
	"errors"
	"testing"

	"github.com/proxysql/orchestrator/go/db"
)

// In a multi-tenant process DefaultStore() must not reach the legacy backend:
// it hands out an inert Store whose database operations fail.
func TestDisabledDefaultStoreFailsWithoutTouchingTheLegacyBackend(t *testing.T) {
	DisableDefaultStore()
	defer EnableDefaultStore()
	db.DisableDefaultBackend()
	defer db.EnableDefaultBackend()

	s := DefaultStore()
	if s.ID() == db.DefaultBackendID {
		t.Fatalf("the inert store must not take the id (nor the metrics) of the legacy tenant")
	}
	if _, _, err := s.ReadInstance(&InstanceKey{Hostname: "h", Port: 3306}); !errors.Is(err, db.ErrDefaultBackendDisabled) {
		t.Errorf("ReadInstance: expected ErrDefaultBackendDisabled, got %v", err)
	}
	if _, err := s.ReadAllInstanceKeys(); !errors.Is(err, db.ErrDefaultBackendDisabled) {
		t.Errorf("ReadAllInstanceKeys: expected ErrDefaultBackendDisabled, got %v", err)
	}
	// the legacy wrappers land on it
	if _, err := ReadAllInstanceKeys(); !errors.Is(err, db.ErrDefaultBackendDisabled) {
		t.Errorf("package ReadAllInstanceKeys: %v", err)
	}
	if DefaultStore() != s {
		t.Errorf("the inert store must be a singleton")
	}
}

func TestDefaultStoreComesBackWhenEnabled(t *testing.T) {
	DisableDefaultStore()
	inert := DefaultStore()
	EnableDefaultStore()
	if got := DefaultStore(); got == inert {
		t.Errorf("EnableDefaultStore must bring the legacy store back")
	}
}
