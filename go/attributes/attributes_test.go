package attributes

import (
	"path/filepath"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

func newTestBackend(t *testing.T, id string) *db.Backend {
	t.Helper()
	cfg := *config.Config
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), id+".sqlite3")
	b, err := db.NewBackend(id, &cfg)
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.EnsureSchema(); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return b
}

// Note: with a SQLite backend `rlike` is translated to `like`, so the match
// patterns below use LIKE wildcards.
func TestStoresAreIsolatedPerBackend(t *testing.T) {
	a := NewStore(newTestBackend(t, "a"))
	b := NewStore(newTestBackend(t, "b"))

	if err := a.SetHostAttributes("host1", "role", "primary"); err != nil {
		t.Fatal(err)
	}
	if err := a.SetGeneralAttribute("domain1", "db1:3306"); err != nil {
		t.Fatal(err)
	}
	if err := b.SetHostAttributes("host1", "role", "replica"); err != nil {
		t.Fatal(err)
	}

	if v, err := a.GetHostAttribute("host1", "role"); err != nil || v != "primary" {
		t.Errorf("a: GetHostAttribute = %q %v", v, err)
	}
	if v, err := b.GetHostAttribute("host1", "role"); err != nil || v != "replica" {
		t.Errorf("b: GetHostAttribute = %q %v", v, err)
	}
	if v, err := a.GetGeneralAttribute("domain1"); err != nil || v != "db1:3306" {
		t.Errorf("a: GetGeneralAttribute = %q %v", v, err)
	}
	if got, err := b.GetHostAttributesByMatch("", "dom%", ""); err != nil || len(got) != 0 {
		t.Errorf("a's general attribute leaked into b: %+v %v", got, err)
	}
	got, err := a.GetHostAttributesByMatch("%host%", "", "%prim%")
	if err != nil || len(got) != 1 || got[0].AttributeValue != "primary" {
		t.Errorf("GetHostAttributesByMatch = %+v %v", got, err)
	}
	got, err = a.GetHostAttributesByAttribute("role", "%")
	if err != nil || len(got) != 1 {
		t.Errorf("GetHostAttributesByAttribute = %+v %v", got, err)
	}
	if got, err := a.GetHostAttributesByMatch("", "", ""); err != nil || len(got) != 2 {
		t.Errorf("unfiltered match = %+v %v", got, err)
	}
	if err := a.SetGeneralAttribute("", "ignored"); err != nil {
		t.Errorf("empty general attribute name must be a no-op: %v", err)
	}
}

func TestLegacyWrappersUseDefaultBackend(t *testing.T) {
	origDB, origFile := config.Config.BackendDB, config.Config.SQLite3DataFile
	defer func() { config.Config.BackendDB, config.Config.SQLite3DataFile = origDB, origFile }()
	config.Config.BackendDB = "sqlite3"
	config.Config.SQLite3DataFile = filepath.Join(t.TempDir(), "default.sqlite3")

	if err := SetHostAttributes("h", "k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := SetGeneralAttribute("g", "gv"); err != nil {
		t.Fatal(err)
	}
	if v, err := GetHostAttribute("h", "k"); err != nil || v != "v" {
		t.Errorf("GetHostAttribute = %q %v", v, err)
	}
	if v, err := GetGeneralAttribute("g"); err != nil || v != "gv" {
		t.Errorf("GetGeneralAttribute = %q %v", v, err)
	}
	if got, err := GetHostAttributesByAttribute("k", "%"); err != nil || len(got) != 1 {
		t.Errorf("GetHostAttributesByAttribute = %+v %v", got, err)
	}
	if got, err := GetHostAttributesByMatch("%h%", "", ""); err != nil || len(got) != 1 {
		t.Errorf("GetHostAttributesByMatch = %+v %v", got, err)
	}
	// Same data through an explicit Store on the default backend.
	if v, err := DefaultStore().GetHostAttribute("h", "k"); err != nil || v != "v" {
		t.Errorf("DefaultStore = %q %v", v, err)
	}
}

func TestGetMissingAttributeReturnsErrorWithoutPanic(t *testing.T) {
	s := NewStore(newTestBackend(t, "missing"))

	if v, err := s.GetHostAttribute("nohost", "noattr"); err == nil || v != "" {
		t.Errorf("GetHostAttribute on a missing attribute = %q, %v; want empty value and error", v, err)
	}
	if v, err := s.GetGeneralAttribute("noattr"); err == nil || v != "" {
		t.Errorf("GetGeneralAttribute on a missing attribute = %q, %v; want empty value and error", v, err)
	}
}
