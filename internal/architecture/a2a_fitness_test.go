package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// Where the A2A code lives. The INBOUND server (ADR-0059) and the OUTBOUND
// work-planning client (ADR-0058) are two separate adapters: they share no
// code, and only the client may touch the SDK's client package.
const (
	a2aInboundDir  = "../adapters/inbound/a2a"
	a2aOutboundDir = "../adapters/outbound/a2a"
	a2aCmdDir      = "../../cmd/a2a"

	a2aClientSDK = "github.com/a2aproject/a2a-go/v2/a2aclient"
	a2aOutboundP = modulePath + "/internal/adapters/outbound/a2a"
)

// a2aAllowedUseCases is the ALLOWLIST of application use cases (and their
// sentinel errors) the A2A server may reference. It mirrors the MCP write
// gate: A2A offers only what MCP already offers, so the only write is
// CompleteTask (the MCP complete_task tool). Widening the delegated surface
// is a conscious edit of this list plus an ADR, not an accident.
var a2aAllowedUseCases = map[string]bool{
	"GetQueueDepth": true,
	"CompleteTask":  true,
	// Sentinel error the problem mapping classifies (REST: 404).
	"ErrTaskNotFound": true,
}

// TestA2AInboundAdapterDependencyRule encodes ADR-0059: the A2A server is an
// additive inbound adapter. It may depend only on the application and domain
// layers (never on an outbound adapter, never on cmd), and nothing else in
// this codebase may depend on it - it stays a pure entrypoint that cmd/a2a
// wires up.
func TestA2AInboundAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("a2a adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.inbound.a2a.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.inbound.a2a.**",
							"**.application.**",
							"**.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the a2a adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.inbound.a2a.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// importPathsOf parses src (ImportsOnly) and returns its import paths.
func importPathsOf(t *testing.T, name, src string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var paths []string
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			t.Fatalf("%s: bad import %s: %v", name, imp.Path.Value, err)
		}
		paths = append(paths, p)
	}
	return paths
}

// usecaseRefsOf parses src and returns the distinct identifiers referenced as
// `usecases.<Name>`. It works on the AST, so comments and string literals can
// neither hide nor fake a reference.
func usecaseRefsOf(t *testing.T, name, src string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	seen := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == "usecases" {
			seen[sel.Sel.Name] = true
		}
		return true
	})
	refs := make([]string, 0, len(seen))
	for r := range seen {
		refs = append(refs, r)
	}
	sort.Strings(refs)
	return refs
}

func readSource(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// forbiddenImports returns the imports of src that match any of banned, as
// exact paths.
func forbiddenImports(t *testing.T, name, src string, banned ...string) []string {
	t.Helper()
	var hits []string
	for _, p := range importPathsOf(t, name, src) {
		for _, b := range banned {
			if p == b {
				hits = append(hits, p)
			}
		}
	}
	return hits
}

// disallowedUseCases returns the usecases.<Name> references of src that are
// not in the allowlist.
func disallowedUseCases(t *testing.T, name, src string) []string {
	t.Helper()
	var bad []string
	for _, r := range usecaseRefsOf(t, name, src) {
		if !a2aAllowedUseCases[r] {
			bad = append(bad, r)
		}
	}
	return bad
}

// TestA2AServerNeverUsesTheClientHalf keeps the two A2A halves apart: neither
// the inbound adapter nor cmd/a2a imports the SDK's client package or the
// outbound work-planning adapter (ADR-0058 is a client of one peer; this
// binary is a server and must not become a relay).
func TestA2AServerNeverUsesTheClientHalf(t *testing.T) {
	var files []string
	files = append(files, goFilesUnder(t, a2aInboundDir, false)...)
	files = append(files, goFilesUnder(t, a2aCmdDir, false)...)
	if len(files) == 0 {
		t.Fatal("found no A2A server sources; the sensor is looking in the wrong place")
	}
	for _, path := range files {
		if hits := forbiddenImports(t, path, readSource(t, path), a2aClientSDK, a2aOutboundP); len(hits) > 0 {
			t.Errorf("%s imports the outbound A2A half %v: the A2A server must not be a client", path, hits)
		}
	}
}

// TestA2AClientSDKIsImportedOnlyByTheOutboundAdapter pins the converse: the
// SDK's client package is reachable only from the outbound adapter, so a
// client cannot creep into another package under cover of the server.
func TestA2AClientSDKIsImportedOnlyByTheOutboundAdapter(t *testing.T) {
	var files []string
	files = append(files, goFilesUnder(t, "..", false)...)
	files = append(files, goFilesUnder(t, "../../cmd", false)...)
	importers := 0
	for _, path := range files {
		if len(forbiddenImports(t, path, readSource(t, path), a2aClientSDK)) == 0 {
			continue
		}
		importers++
		if !strings.HasPrefix(path, a2aOutboundDir+"/") {
			t.Errorf("%s imports %s: only %s may", path, a2aClientSDK, a2aOutboundDir)
		}
	}
	if importers == 0 {
		t.Fatal("no importer of the A2A client SDK found; the outbound adapter moved and the sensor is blind")
	}
}

// TestA2AServerOffersOnlyWhatMCPOffers is the write gate as a sensor: the
// inbound adapter and cmd/a2a may reference only the allowlisted use cases.
func TestA2AServerOffersOnlyWhatMCPOffers(t *testing.T) {
	var files []string
	files = append(files, goFilesUnder(t, a2aInboundDir, false)...)
	files = append(files, goFilesUnder(t, a2aCmdDir, false)...)
	for _, path := range files {
		if bad := disallowedUseCases(t, path, readSource(t, path)); len(bad) > 0 {
			t.Errorf("%s references use cases outside the A2A allowlist %v: A2A may not widen what MCP exposes (ADR-0059)", path, bad)
		}
	}
}

// TestA2ASensorsBite proves the sensors above fire on violations (code
// fixtures, not comments) and stay quiet on legitimate code.
func TestA2ASensorsBite(t *testing.T) {
	const clientImport = `package x
import "github.com/a2aproject/a2a-go/v2/a2aclient"
var _ = a2aclient.NewFromCard
`
	if hits := forbiddenImports(t, "fixture.go", clientImport, a2aClientSDK); len(hits) != 1 {
		t.Errorf("client-SDK import not detected: %v", hits)
	}
	const outboundImport = `package x
import _ "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/a2a"
`
	if hits := forbiddenImports(t, "fixture.go", outboundImport, a2aOutboundP); len(hits) != 1 {
		t.Errorf("outbound-adapter import not detected: %v", hits)
	}
	const serverOnly = `package x
import "github.com/a2aproject/a2a-go/v2/a2asrv"
var _ = a2asrv.NewHandler
`
	if hits := forbiddenImports(t, "fixture.go", serverOnly, a2aClientSDK, a2aOutboundP); len(hits) != 0 {
		t.Errorf("server SDK import wrongly flagged: %v", hits)
	}

	const widened = `package x
var _ = usecases.ClaimNext{}
var _ = usecases.CompleteTask{}
var _ = &usecases.ReleaseTask{}
`
	if bad := disallowedUseCases(t, "fixture.go", widened); len(bad) != 2 || bad[0] != "ClaimNext" || bad[1] != "ReleaseTask" {
		t.Errorf("write-gate widening not detected, got %v", bad)
	}
	const inComment = `package x
// usecases.ClaimNext is mentioned only in a comment
var s = "usecases.ReleaseTask"
var _ = usecases.GetQueueDepth{}
`
	if bad := disallowedUseCases(t, "fixture.go", inComment); len(bad) != 0 {
		t.Errorf("comment or string wrongly flagged: %v", bad)
	}
}
