package architecture

// Runtime-binary sensor (fleet decision 2026-10-19: every context runs only
// on real Postgres and real Kafka; the in-memory adapters are test doubles).
// Ported from the harness template (warehouse-harness-template#33) and
// extended with this service's own in-memory shapes.
//
//   - TestRuntimeBinaryHasNoInMemoryAdapters: no non-test file under cmd/**
//     may import internal/adapters/outbound/memory (or anything below it), nor
//     the log/buffered/multi publishers of internal/adapters/outbound/events,
//     nor reference the in-memory / permissive copies that live inside the
//     productclassificationcopy and interleavinghintcopy adapter packages.
//     Without this rule a cmd can fall back to them when DATABASE_URL is
//     unset, start no relay without KAFKA_BROKERS and leave every consumer off
//     by default, so a misconfigured deployment comes up "healthy" and
//     silently loses every event.
//   - TestRuntimeBinarySensorBites proves the detector flags a plain, an
//     aliased, a blank and a dot import of the memory package and a nested
//     package of it, the events package and the in-memory copy constructors,
//     and passes the production adapters, a look-alike path and a comment that
//     merely mentions the package.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// memoryAdapterSuffix is the import-path tail of the in-memory test doubles.
const memoryAdapterSuffix = "/internal/adapters/outbound/memory"

// eventsAdapterSuffix is the import-path tail of the log / buffered / multi
// publishers: test doubles and debugging aids, not a runtime sink.
const eventsAdapterSuffix = "/internal/adapters/outbound/events"

// forbiddenCopySymbols are the in-memory / permissive implementations that
// live INSIDE a production adapter package, keyed by the package's import-path
// tail. A cmd may import the package (for its Postgres store) but never name
// these.
var forbiddenCopySymbols = map[string][]string{
	"/internal/adapters/outbound/productclassificationcopy": {"NewMemoryStore", "MemoryStore", "NewPermissiveLookup", "PermissiveLookup"},
	"/internal/adapters/outbound/interleavinghintcopy":      {"NewMemoryStore", "MemoryStore"},
}

// isTestDoubleImport reports whether path is the memory adapter package (or a
// package below it) or the events package (or a package below it).
func isTestDoubleImport(path string) bool {
	for _, suffix := range []string{memoryAdapterSuffix, eventsAdapterSuffix} {
		if strings.HasSuffix(path, suffix) || strings.Contains(path, suffix+"/") {
			return true
		}
	}
	return false
}

// localName is the identifier a file refers to an imported package by.
func localName(imp *ast.ImportSpec, path string) string {
	if imp.Name != nil {
		return imp.Name.Name
	}
	return filepath.Base(path)
}

// runtimeDoubles parses src as a Go file and returns one finding per import of
// a test double ("file:line imports path") and one per use of an in-memory
// symbol of a copy package ("file:line uses pkg.Symbol").
func runtimeDoubles(filename string, src any) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	var out []string
	forbiddenByName := map[string]map[string]bool{}
	for _, imp := range f.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		if isTestDoubleImport(path) {
			out = append(out, fmt.Sprintf("%s imports %s", fset.Position(imp.Pos()), path))
		}
		for suffix, symbols := range forbiddenCopySymbols {
			if !strings.HasSuffix(path, suffix) {
				continue
			}
			set := map[string]bool{}
			for _, s := range symbols {
				set[s] = true
			}
			forbiddenByName[localName(imp, path)] = set
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		if forbiddenByName[pkg.Name][sel.Sel.Name] {
			out = append(out, fmt.Sprintf("%s uses %s.%s", fset.Position(sel.Pos()), pkg.Name, sel.Sel.Name))
		}
		return true
	})
	return out, nil
}

func runtimeBinaryViolation(finding string) string {
	return fmt.Sprintf("ARCHITECTURE VIOLATION (contents rule)\n"+
		"  RULE:    the shipped binaries (cmd/**) never use the in-memory adapters, the log/buffered/multi publishers or an in-memory/permissive copy\n"+
		"  WHAT:    %s\n"+
		"  WHY:     this context runs on real Postgres and real Kafka only. An in-memory runtime path "+
		"lets a misconfigured deployment boot \"healthy\" with no outbox relay, no consumers and no idempotency, "+
		"and silently lose events.\n"+
		"  FIX:     wire the Postgres/Kafka adapters instead; if the code you need is a clock it "+
		"lives in internal/adapters/outbound/system. The memory adapters stay in tests (unit and BDD). "+
		"Do NOT loosen this test.\n", finding)
}

func TestRuntimeBinaryHasNoInMemoryAdapters(t *testing.T) {
	cmdDir := filepath.Join("..", "..", "cmd")
	files := goFilesUnder(t, cmdDir, false)
	if len(files) == 0 {
		t.Fatal("cmd/ holds no non-test Go file: the sensor would pass vacuously")
	}
	sort.Strings(files)
	for _, path := range files {
		findings, err := runtimeDoubles(path, nil)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, finding := range findings {
			t.Errorf("%s", runtimeBinaryViolation(finding))
		}
	}
}

func TestRuntimeBinarySensorBites(t *testing.T) {
	const mem = `"example.com/svc/internal/adapters/outbound/memory"`
	cases := []struct {
		name string
		src  string
		want int
	}{
		{"plain import is flagged", "package main\nimport " + mem + "\nvar _ = memory.NewTaskRepo", 1},
		{"aliased import is flagged", "package main\nimport mem " + mem, 1},
		{"blank import is flagged", "package main\nimport _ " + mem, 1},
		{"dot import is flagged", "package main\nimport . " + mem, 1},
		{"a package below memory is flagged", "package main\nimport \"example.com/svc/internal/adapters/outbound/memory/nested\"", 1},
		{"another module's memory adapter is flagged", "package main\nimport \"github.com/other/svc/internal/adapters/outbound/memory\"", 1},
		{"two imports are two findings", "package main\nimport (\n" + mem + "\nm2 \"github.com/other/svc/internal/adapters/outbound/memory\"\n)", 2},
		{"the log publisher package is flagged", "package main\nimport \"example.com/svc/internal/adapters/outbound/events\"\nvar _ = events.NewLogPublisher", 1},
		{"an in-memory classification copy is flagged", `package main
import "example.com/svc/internal/adapters/outbound/productclassificationcopy"
var _ = productclassificationcopy.NewMemoryStore()`, 1},
		{"a permissive classification lookup is flagged", `package main
import pcc "example.com/svc/internal/adapters/outbound/productclassificationcopy"
var _ = pcc.NewPermissiveLookup()`, 1},
		{"an in-memory hint copy is flagged", `package main
import "example.com/svc/internal/adapters/outbound/interleavinghintcopy"
var _ = interleavinghintcopy.NewMemoryStore()`, 1},
		{"the Postgres copies pass", `package main
import (
	"example.com/svc/internal/adapters/outbound/interleavinghintcopy"
	"example.com/svc/internal/adapters/outbound/productclassificationcopy"
)
var _ = productclassificationcopy.NewPostgresStore
var _ = interleavinghintcopy.NewPostgresStore`, 0},
		{"the production adapters pass", `package main
import (
	"example.com/svc/internal/adapters/outbound/postgres"
	"example.com/svc/internal/adapters/outbound/system"
)`, 0},
		{"a look-alike path passes", "package main\nimport \"example.com/svc/internal/adapters/outbound/memorystore\"", 0},
		{"a comment that names the package passes", "package main\n// the memory adapters (internal/adapters/outbound/memory) are test doubles\nimport \"fmt\"\nvar _ = fmt.Sprint", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runtimeDoubles("src.go", tc.src)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(got) != tc.want {
				t.Fatalf("findings = %v, want %d", got, tc.want)
			}
		})
	}
}

// The sensor really scans this repo's binaries (the file walk is not empty
// and reaches every cmd directory).
func TestRuntimeBinarySensorCoversEveryCmd(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("..", "..", "cmd"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if files := goFilesUnder(t, filepath.Join("..", "..", "cmd", e.Name()), false); len(files) == 0 {
			t.Errorf("cmd/%s has no non-test Go file: the sensor reaches nothing there", e.Name())
		}
	}
}
