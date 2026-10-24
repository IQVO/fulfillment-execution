package gs1

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// sourceDigest pins every other .go file in this package (sources AND tests). This package is a
// byte-identical copy of warehouse-harness-template's internal/domain/shared/gs1; a service that
// edits its copy fails here, and the template's drift sensor (scripts/harness-health.py, fed by
// GS1_PACKAGE in tools/migrate_v3.py) flags it too.
//
// Changing the package: edit the TEMPLATE, run this test there, paste the digest it prints into
// this constant, then re-sync every consumer with `python3 tools/migrate_v3.py --repo <svc>`.
const sourceDigest = "8c2ed983baaface28bea37363af33779bae61061c90dbafe1dff4d0aaf772f3f"

func TestPackageIsByteIdenticalToTemplate(t *testing.T) {
	got := digestOfPackageFiles(t)
	if got != sourceDigest {
		t.Fatalf("WHAT: internal/domain/shared/gs1 differs from the template's canonical copy.\n"+
			"WHY:  GTIN/SSCC validation must accept and reject exactly the same input in every service.\n"+
			"FIX:  do not edit this package here. Change warehouse-harness-template first, update sourceDigest there "+
			"(this run computed %s), then re-sync with tools/migrate_v3.py.", got)
	}
}

func digestOfPackageFiles(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasSuffix(n, ".go") && n != "checksum_test.go" {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		b, err := os.ReadFile(filepath.Clean(n))
		if err != nil {
			t.Fatal(err)
		}
		h.Write([]byte(n + "\x00"))
		h.Write(b)
		h.Write([]byte("\x00"))
	}
	return hex.EncodeToString(h.Sum(nil))
}
