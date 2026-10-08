package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sqliteTenants(a, b string) []byte {
	entry := func(id, file string) string {
		return `{"ID":"` + id + `","Provider":"mysql","Backend":{"BackendDB":"sqlite3","SQLite3DataFile":"` + file + `"}}`
	}
	return []byte(`{"Tenants":[` + entry("t1", a) + `,` + entry("t2", b) + `]}`)
}

func TestSQLiteSharedFileDetectedThroughAliases(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "a.db")
	link := filepath.Join(dir, "link.db")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(dir, "linkdir")
	if err := os.Symlink(dir, linkDir); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(wd, real)
	if err != nil {
		t.Fatal(err)
	}

	for name, other := range map[string]string{
		"symlink to the file":            link,
		"symlinked directory (new file)": filepath.Join(linkDir, "a.db"),
		"dot-dot":                        filepath.Join(dir, "x", "..", "a.db"),
		"relative":                       rel,
	} {
		base := newConfiguration()
		_, err := ParseTenantConfigs(base, sqliteTenants(real, other))
		if err == nil || !strings.Contains(err.Error(), "SQLite3DataFile") {
			t.Errorf("%s: expected a shared SQLite error, got %v", name, err)
		}
	}

	// the file existing or not must not matter, and distinct files stay valid
	if err := os.WriteFile(real, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseTenantConfigs(newConfiguration(), sqliteTenants(real, link)); err == nil {
		t.Error("symlink to an existing file must be detected")
	}
	if _, err := ParseTenantConfigs(newConfiguration(), sqliteTenants(real, filepath.Join(dir, "b.db"))); err != nil {
		t.Errorf("distinct files must be accepted: %v", err)
	}
}

func TestSQLiteTenantMustNotUseControlBackendFile(t *testing.T) {
	dir := t.TempDir()
	base := newConfiguration()
	base.BackendDB = "sqlite3"
	base.SQLite3DataFile = filepath.Join(dir, "control.db")

	// a single tenant inheriting the node's file
	_, err := ParseTenantConfigs(base, []byte(`{"Tenants":[{"ID":"a","Provider":"mysql"}]}`))
	if err == nil || !strings.Contains(err.Error(), "control") {
		t.Errorf("a tenant inheriting the control SQLite file must be rejected, got %v", err)
	}
	// explicitly naming it, through an alias
	link := filepath.Join(dir, "alias.db")
	if err := os.Symlink(base.SQLite3DataFile, link); err != nil {
		t.Fatal(err)
	}
	_, err = ParseTenantConfigs(base, sqliteTenants(link, filepath.Join(dir, "b.db")))
	if err == nil || !strings.Contains(err.Error(), "control") {
		t.Errorf("a tenant naming the control SQLite file must be rejected, got %v", err)
	}
	// its own file is fine
	if _, err := ParseTenantConfigs(base, sqliteTenants(filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db"))); err != nil {
		t.Errorf("own files must be accepted: %v", err)
	}
}

func TestSQLiteTenantMustNotUseRaftStore(t *testing.T) {
	dir := t.TempDir()
	base := newConfiguration()
	base.RaftEnabled = true
	base.RaftDataDir = dir
	_, err := ParseTenantConfigs(base, sqliteTenants(filepath.Join(dir, "raft_store.db"), filepath.Join(dir, "b.db")))
	if err == nil || !strings.Contains(err.Error(), "Raft") {
		t.Errorf("expected a Raft store error, got %v", err)
	}
}
