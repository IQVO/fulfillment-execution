package http_test

import (
	stdhttp "net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
)

// openAPIOperations returns every "METHOD /path" operation declared in
// apis/openapi.yaml, located relative to this source file so the test
// does not depend on the working directory `go test` runs from.
func openAPIOperations(t *testing.T) map[string]bool {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	specPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "apis", "openapi.yaml")
	raw, err := os.ReadFile(specPath) // #nosec G304 -- fixed repo-relative path, test-only
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse %s: %v", specPath, err)
	}
	httpMethods := map[string]bool{
		"get": true, "put": true, "post": true, "delete": true,
		"options": true, "head": true, "patch": true, "trace": true,
	}
	ops := map[string]bool{}
	for path, item := range spec.Paths {
		for method := range item {
			if httpMethods[method] {
				ops[strings.ToUpper(method)+" "+path] = true
			}
		}
	}
	return ops
}

// routerOperations returns every "METHOD /path" route registered on the
// OLTP chi router, built exactly as the composition root builds it.
func routerOperations(t *testing.T) map[string]bool {
	t.Helper()
	h, _, _, _, _ := newTestHandlers()
	ops := map[string]bool{}
	err := chi.Walk(http.NewRouter(h, nil), func(method, route string, _ stdhttp.Handler, _ ...func(stdhttp.Handler) stdhttp.Handler) error {
		ops[method+" "+route] = true
		return nil
	})
	if err != nil {
		t.Fatalf("chi.Walk: %v", err)
	}
	return ops
}

func missingFrom(want, have map[string]bool) []string {
	var out []string
	for op := range want {
		if !have[op] {
			out = append(out, op)
		}
	}
	sort.Strings(out)
	return out
}

// TestOpenAPISpec_CoversEveryRouterRoute guards the contract drift the
// docs-api-drift CI job cannot see: a route that is live on the chi router
// but absent from apis/openapi.yaml (POST /rebin/arrivals was exactly
// that) has no generated reference page, no Schemathesis coverage, and is
// invisible to any client driven purely by the published contract.
func TestOpenAPISpec_CoversEveryRouterRoute(t *testing.T) {
	if missing := missingFrom(routerOperations(t), openAPIOperations(t)); len(missing) > 0 {
		t.Errorf("routes registered on the router but not declared in apis/openapi.yaml: %v", missing)
	}
}

// TestOpenAPISpec_DeclaresNoUnroutedOperation is the reverse direction: a
// spec operation with no router route is a contract the service cannot
// honour (every call would 404/405).
func TestOpenAPISpec_DeclaresNoUnroutedOperation(t *testing.T) {
	if missing := missingFrom(openAPIOperations(t), routerOperations(t)); len(missing) > 0 {
		t.Errorf("operations declared in apis/openapi.yaml but not registered on the router: %v", missing)
	}
}
