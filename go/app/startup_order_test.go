package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The tenants (and Raft with them) must be running before the process starts
// serving: a request, or a Raft command replayed into a tenant that is not ready,
// would otherwise be lost. In Http() the manager is built first; in standardHttp
// startManager (which sets Raft up) must come before every listener.
func TestManagerIsBuiltAndStartedBeforeServing(t *testing.T) {
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, "http.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	positions := func(fn string) map[string][]token.Pos {
		out := map[string][]token.Pos{}
		for _, d := range af.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Name.Name != fn {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.Ident:
					out[f.Name] = append(out[f.Name], call.Pos())
				case *ast.SelectorExpr:
					out[f.Sel.Name] = append(out[f.Sel.Name], call.Pos())
				}
				return true
			})
		}
		return out
	}

	std := positions("standardHttp")
	starts := std["startManager"]
	if len(starts) != 1 {
		t.Fatalf("standardHttp must call startManager exactly once, found %d", len(starts))
	}
	for _, listener := range []string{"ListenAndServe", "ListenAndServeTLS", "Serve", "Listen"} {
		for _, pos := range std[listener] {
			if pos < starts[0] {
				t.Errorf("%s: %s before startManager: the process would serve before its tenants and Raft are up", fset.Position(pos), listener)
			}
		}
	}

	top := positions("Http")
	builds, standard := top["buildProcessManager"], top["standardHttp"]
	if len(builds) != 1 || len(standard) != 1 || builds[0] > standard[0] {
		t.Fatalf("Http must build the manager (all tenants) before standardHttp")
	}
	for _, pos := range top["agentsHttp"] {
		if pos < builds[0] {
			t.Errorf("the agents listener must not start before the manager is built")
		}
	}
}
