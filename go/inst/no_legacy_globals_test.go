package inst

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// legacySelectors are the package-level entry points of the single-tenant world:
// the default backend/store, the global config and the shims over them.
var legacySelectors = map[string]map[string]bool{
	"db":         {"DefaultBackend": true, "ExecOrchestrator": true, "QueryOrchestrator": true, "QueryOrchestratorRowsMap": true, "QueryOrchestratorBuffered": true, "QueryOrchestratorRowsMapBuffered": true, "OpenOrchestrator": true, "OpenTopology": true, "OpenDiscovery": true, "ReadTimeNow": true, "IsSQLite": true},
	"config":     {"Config": true},
	"kv":         {"GetValue": true, "PutValue": true, "PutKVPairs": true, "DistributePairs": true, "InitKVStores": true, "DefaultStores": true},
	"attributes": {"SetHostAttributes": true, "GetHostAttribute": true, "GetHostAttributesByMatch": true, "SetGeneralAttribute": true, "GetGeneralAttribute": true, "GetHostAttributesByAttribute": true, "DefaultStore": true},
	"process":    {"DefaultControl": true},
	"proxysql":   {"GetHook": true, "InitHook": true},
	"inst":       {"DefaultStore": true},
}

func parseGoFiles(t *testing.T, fset *token.FileSet, dir string, include func(name string) bool) map[string]*ast.File {
	t.Helper()
	files := map[string]*ast.File{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || !include(filepath.Base(path)) {
			return nil
		}
		af, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		files[path] = af
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func isProduction(name string) bool {
	return !strings.HasSuffix(name, "_test.go") && !strings.HasSuffix(name, "_compat.go")
}

// TestNoLegacyGlobalsOutsideCompat fails when tenant-aware code of this package
// reaches the single-tenant world. Only *_compat.go and default_store_compat.go
// may do it.
func TestNoLegacyGlobalsOutsideCompat(t *testing.T) {
	fset := token.NewFileSet()
	// The constructors NewMySQLProvider()/NewPostgreSQLProvider() keep their
	// pre-multi-tenant variadic signature: without a configuration they fall back
	// on the global one. Nothing but tests calls them that way (NewStore always
	// passes its tenant's configuration; see TestNoLegacyCallersOutsideInst).
	legacyConfigAllowed := map[string]bool{"provider_mysql.go": true, "provider_postgresql.go": true}
	for path, af := range parseGoFiles(t, fset, ".", isProduction) {
		name := filepath.Base(path)
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && id.Name != "inst" && legacySelectors[id.Name][x.Sel.Name] {
					t.Errorf("%s: %s.%s used outside *_compat.go", fset.Position(x.Pos()), id.Name, x.Sel.Name)
				}
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && (id.Name == "DefaultStore" || id.Name == "legacyConfig") &&
					name != "store.go" && !(id.Name == "legacyConfig" && legacyConfigAllowed[name]) {
					t.Errorf("%s: %s() used outside *_compat.go", fset.Position(x.Pos()), id.Name)
				}
			}
			return true
		})
	}
}

// TestNoLegacyCallersOutsideInst guards the packages that run per tenant (logic,
// http, app, cmd): none may call the deprecated single-tenant shims -- the
// package-level wrappers or the methods of *Instance, *InstanceKey, ... that
// delegate to inst.DefaultStore() -- nor the legacy defaults directly. In a
// multi-tenant process those would act on the base configuration and the legacy
// backend instead of the tenant's.
//
// Allowed, each for a reason:
//   - logic/tenant.go: builds the legacy "default" tenant on the process defaults.
//   - logic/tenant_manager.go: process.DefaultControl() of the legacy tenant.
//   - http/agents_api.go and go/agent: the agents subsystem is single-tenant (the
//     process refuses ServeAgentsHttp together with a tenants file).
//   - config.Config is the BASE configuration (node-local and global settings), a
//     legitimate read in every package.
func TestNoLegacyCallersOutsideInst(t *testing.T) {
	fset := token.NewFileSet()

	// The deprecated shims of this package: the package-level functions, and the
	// methods whose name no other declaration uses (a name shared with, say, a
	// Store method is ambiguous without type information and is not checked).
	compatFuncs := map[string]bool{}
	compatMethods := map[string]bool{}
	for _, af := range parseGoFiles(t, fset, ".", func(n string) bool { return strings.HasSuffix(n, "_compat.go") && !strings.HasSuffix(n, "_test.go") }) {
		for _, d := range af.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.IsExported() {
				if fd.Recv == nil {
					compatFuncs[fd.Name.Name] = true
				} else {
					compatMethods[fd.Name.Name] = true
				}
			}
		}
	}
	for _, dir := range []string{".", "../db", "../logic", "../http", "../app", "../process", "../kv", "../attributes", "../discovery", "../proxysql", "../config", "../metrics", "../util"} {
		for _, af := range parseGoFiles(t, fset, dir, func(n string) bool { return !strings.HasSuffix(n, "_test.go") && !strings.HasSuffix(n, "_compat.go") }) {
			for _, d := range af.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok {
					delete(compatMethods, fd.Name.Name)
				}
			}
		}
	}
	if len(compatFuncs) < 100 || len(compatMethods) < 10 {
		t.Fatalf("only %d shim functions and %d shim methods found: the scan is not working", len(compatFuncs), len(compatMethods))
	}

	allowedFiles := map[string]bool{
		"logic/tenant.go":         true,
		"logic/tenant_manager.go": true,
		"http/agents_api.go":      true,
	}
	var found []string
	for _, dir := range []string{"../logic", "../http", "../app", "../cmd"} {
		for path, af := range parseGoFiles(t, fset, dir, isProduction) {
			rel := strings.TrimPrefix(filepath.ToSlash(path), "../")
			if allowedFiles[rel] {
				continue
			}
			ast.Inspect(af, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && legacySelectors[id.Name][sel.Sel.Name] && id.Name != "config" {
					found = append(found, fset.Position(sel.Pos()).String()+": "+id.Name+"."+sel.Sel.Name)
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "inst" && compatFuncs[sel.Sel.Name] {
					found = append(found, fset.Position(sel.Pos()).String()+": inst."+sel.Sel.Name+" (deprecated single-tenant shim)")
				} else if compatMethods[sel.Sel.Name] {
					found = append(found, fset.Position(sel.Pos()).String()+": ."+sel.Sel.Name+" (deprecated single-tenant shim method)")
				}
				return true
			})
		}
	}
	sort.Strings(found)
	for _, f := range found {
		t.Errorf("legacy single-tenant call: %s", f)
	}
}
