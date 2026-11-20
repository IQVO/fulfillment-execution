package inventorystorage

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The table is the consumer half of the fleet idempotency rule. These tests keep it honest
// against the byte-pinned copy of the callee's OpenAPI (contracts/pinned.json) and a copy of
// the class table of the callee's ADR 0053.
//
// What the pin can prove, and what it cannot: it names the problem types the case-breaks
// operation documents and, in the Idempotency-Key text, those that are NOT replayed (the key
// is released). It does not enumerate the callee's catalogue; the class table lives in the
// callee's ADR, copied under testdata/ and compared entry by entry.

const (
	pinnedSpec    = "testdata/inventory-storage.openapi.yaml"
	adrClassesTSV = "testdata/inventory-storage-adr-0053-classes.tsv"
	caseBreakPath = "/containers/{lpn}/case-breaks"
)

var (
	parenSlug    = regexp.MustCompile(`\(([a-z]+(?:-[a-z]+)+)\)`)
	backtickSlug = regexp.MustCompile("`([a-z]+(?:-[a-z]+)+)`")
)

func asMap(t *testing.T, v any, what string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("the pinned spec has no %s", what)
	}
	return m
}

func asMapOrNil(v any) map[string]any { m, _ := v.(map[string]any); return m }

func loadSpec(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(pinnedSpec)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func squash(v any) string { s, _ := v.(string); return strings.Join(strings.Fields(s), " ") }

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// documentedSlugs are the problem types the 4xx responses of POST /containers/{lpn}/case-breaks
// name: in a response description as `(slug)` and as the `type` of an example.
func documentedSlugs(t *testing.T, doc map[string]any) []string {
	t.Helper()
	paths := asMap(t, doc["paths"], "paths")
	op := asMap(t, asMap(t, paths[caseBreakPath], caseBreakPath)["post"], "POST "+caseBreakPath)
	found := map[string]bool{}
	for status, r := range asMap(t, op["responses"], "responses") {
		if !strings.HasPrefix(status, "4") {
			continue
		}
		resp := asMap(t, r, "response "+status)
		for _, m := range parenSlug.FindAllStringSubmatch(squash(resp["description"]), -1) {
			found[m[1]] = true
		}
		content, _ := resp["content"].(map[string]any)
		for _, mt := range content {
			media := asMap(t, mt, "media type")
			values := []any{media["example"]}
			examples, _ := media["examples"].(map[string]any)
			for _, ex := range examples {
				values = append(values, asMap(t, ex, "example")["value"])
			}
			for _, v := range values {
				if typ := squash(asMapOrNil(v)["type"]); typ != "" {
					found[typ[strings.LastIndex(typ, "/")+1:]] = true
				}
			}
		}
	}
	return sortedKeys(found)
}

// releasedSlugs are the problem types the Idempotency-Key text says are not replayed.
func releasedSlugs(t *testing.T, doc map[string]any) []string {
	t.Helper()
	params := asMap(t, asMap(t, doc["components"], "components")["parameters"], "parameters")
	text := squash(asMap(t, params["IdempotencyKey"], "IdempotencyKey")["description"])
	i, j := strings.Index(text, "What is NOT replayed"), strings.Index(text, "the key is released")
	if i < 0 || j < i {
		t.Fatal("the Idempotency-Key text no longer lists the refusals that are not replayed: read the new text and update this test and the table together")
	}
	found := map[string]bool{}
	for _, m := range backtickSlug.FindAllStringSubmatch(text[i:j], -1) {
		found[m[1]] = true
	}
	return sortedKeys(found)
}

// auditTable returns what is wrong between a table and the pin's reading.
func auditTable(table map[string]refusalClass, documented, released []string) []string {
	var problems []string
	for _, s := range released {
		if c, ok := table[s]; ok && c != classRetryable {
			problems = append(problems, "spec releases "+s+" under the key but the table does not map it retryable")
		}
	}
	for _, s := range documented {
		if table[s] == classUnset {
			problems = append(problems, "spec documents "+s+" on the case break and the table classifies it neither retryable nor terminal")
		}
	}
	return problems
}

func TestEveryProblemTypeThePinnedSpecDocumentsOnTheCaseBreakIsClassified(t *testing.T) {
	doc := loadSpec(t)
	documented, released := documentedSlugs(t, doc), releasedSlugs(t, doc)
	if len(documented) < 10 || len(released) < 8 {
		t.Fatalf("read %d documented and %d released problem types from the pin; the extraction is broken", len(documented), len(released))
	}
	if problems := auditTable(caseBreakRefusals, documented, released); len(problems) > 0 {
		t.Errorf("\n  %s", strings.Join(problems, "\n  "))
	}
}

// A released slug the break documents must be in the table, as retryable.
func TestEveryDocumentedSlugTheSpecReleasesIsInTheTableAsRetryable(t *testing.T) {
	doc := loadSpec(t)
	releasedSet := map[string]bool{}
	for _, s := range releasedSlugs(t, doc) {
		releasedSet[s] = true
	}
	n := 0
	for _, s := range documentedSlugs(t, doc) {
		if releasedSet[s] {
			n++
			if !releasedRefusal(s) {
				t.Errorf("the spec documents %s on the case break and says its key is released, but the table does not retry it", s)
			}
		}
	}
	if n < 5 {
		t.Fatalf("only %d documented released slugs found; the extraction is broken", n)
	}
}

// The audit itself must fail when a table drifts.
func TestTheAuditFailsWhenTheTableDrifts(t *testing.T) {
	doc := loadSpec(t)
	documented, released := documentedSlugs(t, doc), releasedSlugs(t, doc)
	drifted := map[string]refusalClass{}
	for k, v := range caseBreakRefusals {
		drifted[k] = v
	}
	delete(drifted, "empty-bin-id")
	drifted["concurrent-modification"] = classTerminal
	got := strings.Join(auditTable(drifted, documented, released), "\n")
	for _, want := range []string{"empty-bin-id", "concurrent-modification"} {
		if !strings.Contains(got, want) {
			t.Errorf("the audit missed %s:\n%s", want, got)
		}
	}
}

func adrClasses(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(adrClassesTSV)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 3 {
			t.Fatalf("bad line %q", line)
		}
		out[f[0]] = f[2]
	}
	return out
}

// Every table entry has the class of the callee's merged ADR 0053 table: RETRYABLE is
// retryable, FINAL and OUTCOME are terminal. The copy holds 81 problem types: 44 FINAL,
// 2 OUTCOME, 35 RETRYABLE.
func TestTheTableCarriesTheClassesOfTheCalleesADRTable(t *testing.T) {
	adr := adrClasses(t)
	if len(adr) != 81 {
		t.Fatalf("the ADR copy has %d problem types, want 81", len(adr))
	}
	for slug, class := range caseBreakRefusals {
		want, ok := adr[slug]
		if !ok {
			t.Errorf("%s is in the table but not in the callee's ADR class table", slug)
			continue
		}
		if (want == "RETRYABLE") != (class == classRetryable) || (class != classRetryable && class != classTerminal) {
			t.Errorf("%s: the callee's class is %s, the table says %d", slug, want, class)
		}
	}
}

// The slugs a break can answer, read from the callee's break_case.go and its placement checks
// (and those the brief named): each is explicitly classified, with the callee's class.
func TestTheTableNamesEverySlugTheBreakCanAnswer(t *testing.T) {
	adr := adrClasses(t)
	reachable := []string{
		"container-not-found", "bin-not-found", "stock-unit-not-found", "container-not-sealed", "container-not-stowed",
		"container-reserved", "break-not-conserving", "container-not-at-site", "bin-handling-unit-mismatch", "bin-full",
		"release-exceeds-occupancy", "concurrent-modification", "hazmat-zone-required", "hazmat-class-incompatible",
		"temperature-class-mismatch", "location-classification-unavailable", "product-classification-not-found",
		"held-unit-closed", "transfer-receipt-conflict",
		"empty-bin-id", "malformed-request-body", "idempotency-key-required", "idempotency-key-invalid", "idempotency-key-reused", "not-found",
	}
	for _, s := range reachable {
		if caseBreakRefusals[s] == classUnset {
			t.Errorf("%s is not classified", s)
		} else if (adr[s] == "RETRYABLE") != releasedRefusal(s) {
			t.Errorf("%s: callee class %s, retried here: %v", s, adr[s], releasedRefusal(s))
		}
	}
}

func TestEveryTableEntryIsExplicit(t *testing.T) {
	if len(caseBreakRefusals) == 0 {
		t.Fatal("the table is empty")
	}
	for slug, class := range caseBreakRefusals {
		if class != classRetryable && class != classTerminal {
			t.Errorf("%s has class %d", slug, class)
		}
	}
}

func TestAnUnknownSlugIsNotReleased(t *testing.T) {
	for _, s := range []string{"", "no-such-problem", "internal-error", "empty-bin-id", "idempotency-key-reused"} {
		if releasedRefusal(s) {
			t.Errorf("%q must be terminal", s)
		}
	}
}
