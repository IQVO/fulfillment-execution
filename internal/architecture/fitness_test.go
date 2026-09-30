// Package architecture holds fitness tests (the Go analogue of ArchUnit,
// via github.com/arch-go/arch-go) that enforce the hexagonal/ports-and-adapters
// dependency rule described in the project's CLAUDE.md: dependencies point
// inward only, and inbound/outbound adapters never depend on each other.
package architecture

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestMCPAdapterDependencyRule encodes ADR-0008: the MCP inbound adapter is
// additive, never load-bearing for the OLTP composition root. It may depend
// only on the application and domain layers (never on outbound adapters,
// never on cmd), and — the direction that actually matters for keeping it
// additive — nothing else in this codebase may depend on it. If some other
// package started importing internal/adapters/inbound/mcp, the MCP surface
// would have silently become a dependency other code relies on rather than
// a pure inbound entrypoint cmd/mcp wires up and nothing else touches.
func TestMCPAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("mcp adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.inbound.mcp.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.inbound.mcp.**",
							"**.application.**",
							"**.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the mcp adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.inbound.mcp.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// assertArchGoPasses is a shared helper for the two-return-shape arch-go
// result (DependenciesRuleResult here; the file's other tests use their own
// assertPasses with the same semantics — kept separate to avoid touching
// existing tests' helper functions).
func assertArchGoPasses(t *testing.T, result *archgo.Result) {
	t.Helper()

	if result.Pass {
		return
	}

	if result.DependenciesRuleResult != nil {
		for _, r := range result.DependenciesRuleResult.Results {
			if r.Passes {
				continue
			}
			for _, v := range r.Verifications {
				if v.Passes {
					continue
				}
				for _, d := range v.Details {
					t.Errorf("%s: %s", v.Package, d)
				}
			}
		}
	}

	t.FailNow()
}

// ---------------------------------------------------------------------
// Source-scanning fitness tests below. arch-go's DSL only expresses import
// graph shape; these three invariants are about literal content (a string,
// a missing import) so they're enforced the same way
// internal/architecture/fitness_test.go does it in inventory-storage: walk
// the real .go files under internal/, skip generated/_test.go where noted,
// and fail with the exact file:line that violates the rule.
// ---------------------------------------------------------------------

// goFilesUnder returns every non-test .go file under root (relative to the
// module root), or every .go file including tests when includeTests is true.
func goFilesUnder(t *testing.T, root string, includeTests bool) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !includeTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// authPatternRE matches the literal shapes a reintroduced bearer/JWT/API-key
// auth middleware would contain in source. Deliberately narrow (case
// sensitive on "Bearer " and "Authorization" header checks, exact import
// paths for known JWT libraries) so it doesn't false-positive on the
// existing CORS AllowedHeaders: []string{"Authorization"} allowlist entry
// or on the historical/comment-only mentions of the fleet's auth revert —
// this test scans non-comment, non-test Go source only, and every current
// hit in this codebase is exactly those two known-safe shapes, verified
// clean before this test was added.
var authPatternRE = regexp.MustCompile(`"Bearer |golang-jwt/jwt|dgrijalva/jwt-go|lestrrat-go/jwx`)

// TestNoAuthMiddlewareReintroduced encodes ADR-0022 (2026-09-09 fleet-wide
// decision, adopted here 2026-09-11): the static-bearer auth layer
// (ADR-0021) was removed from this service's REST + MCP surfaces; every
// endpoint is deliberately unauthenticated pending a fresh auth-model
// decision. An agent "helpfully" re-adding a bearer/JWT middleware to
// internal/adapters/inbound should fail CI, not ship silently. This does
// not flag the CORS AllowedHeaders allowlist (which legitimately lists
// "Authorization" as a header name a browser may send, not a check this
// service performs) or doc comments that reference the revert in prose.
func TestNoAuthMiddlewareReintroduced(t *testing.T) {
	for _, path := range goFilesUnder(t, "../adapters/inbound", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if authPatternRE.MatchString(line) {
				t.Errorf("%s:%d: matches a reintroduced-auth pattern: %q — the fleet-wide static-bearer auth layer was deliberately reverted 2026-09-11 per ADR-0022 (unauthenticated pending a fresh auth-model decision); if this is intentional, update this test alongside the ADR documenting the new decision", path, lineNo, trimmed)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// groupIDLiteralRE matches a kafka-go ReaderConfig's GroupID field being
// assigned a bare double-quoted string literal, e.g. `GroupID: "my-group"`.
// A symbol reference (`GroupID: AnalyticsConsumerGroup`, `GroupID: groupID`,
// `GroupID: uniqueConsumerGroup()`) never matches this.
var groupIDLiteralRE = regexp.MustCompile(`GroupID:\s*"[^"]+"`)

// TestKafkaConsumerGroupNeverHardcodedInline encodes a real, already-lived
// fleet incident: wes-work-planning's OLTP consumer group was a hardcoded
// literal (`"wes-work-planning"`), which made a locally-run e2e-tests
// process join the SAME consumer group as the live in-cluster Deployment on
// the shared fleet Kafka broker — Kafka's rebalance protocol then handed the
// single partition to only one of the two group members, silently starving
// whichever process lost the race (fixed fleet-wide in wes-work-planning#67
// by making the group id env-configurable). This service already avoided
// that shape: cmd/execution/main.go reads its OLTP consumer group from the
// WORK_RELEASED_CONSUMER_GROUP env var (via getenv, defaulting to
// "fulfillment-execution"), the analytics projector uses the named
// AnalyticsConsumerGroup const (a deliberate, correct exception — only one
// instance of that consumer ever runs, so there's nothing to collide with),
// and the process-path catalogue consumer uses uniqueConsumerGroup() for
// its per-process-replay pattern. This test doesn't ban long-lived named
// consumer groups outright — it bans the shape that caused the incident: a
// GroupID assigned directly from an inline string literal rather than
// through a named symbol (const, var, env lookup, or function call) that a
// reviewer can trace back to its definition and reasoning.
func TestKafkaConsumerGroupNeverHardcodedInline(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			if groupIDLiteralRE.MatchString(line) {
				t.Errorf("%s:%d: GroupID assigned an inline string literal: %q — use a named const/var, an env lookup, or a function call (see cmd/execution/main.go's WORK_RELEASED_CONSUMER_GROUP getenv pattern, internal/adapters/outbound/kafkacatalog's uniqueConsumerGroup() for the per-process-unique pattern, or the AnalyticsConsumerGroup const for the single-instance pattern) so the group id's lifetime/uniqueness reasoning is traceable and a locally-run process can never silently join a live cluster's group", path, lineNo, strings.TrimSpace(line))
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// TestKafkaIntegrationTestsUseTestcontainers encodes the fleet-wide rule
// (corrected 2026-09-06): a Kafka-touching `-tags=integration` test MUST
// start its own broker via testcontainers-go/modules/kafka, never gate on
// os.Getenv("KAFKA_BROKERS") + t.Skip, and never hardcode localhost:9092.
// This fleet's CI `integration` job provisions Postgres only — a
// skip-gated Kafka test silently skips in CI and proves nothing there,
// while testcontainers is the only variant that actually exercises the
// Kafka assertions on a runner. Scans every _integration_test.go file that
// imports segmentio/kafka-go (this fleet's Kafka client) or references
// GroupID/kafka.Reader/kafka.Writer, and requires it to also import
// testcontainers-go/modules/kafka.
func TestKafkaIntegrationTestsUseTestcontainers(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", true) {
		if !strings.HasSuffix(path, "_integration_test.go") {
			continue
		}

		assertIntegrationTestUsesTestcontainers(t, path)
	}
}

// assertIntegrationTestUsesTestcontainers applies the fleet-wide rule to
// one file: a Kafka-touching `-tags=integration` test MUST start its own
// broker via testcontainers-go/modules/kafka, never gate on
// os.Getenv("KAFKA_BROKERS") + t.Skip, and never hardcode localhost:9092.
// This fleet's CI `integration` job provisions Postgres only — a
// skip-gated Kafka test silently skips in CI and proves nothing there,
// while testcontainers is the only variant that actually exercises the
// Kafka assertions on a runner.
func assertIntegrationTestUsesTestcontainers(t *testing.T, path string) {
	t.Helper()

	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	content := string(src)

	if !integrationTestTouchesKafka(content) {
		return
	}

	hasSkipGate, hasHardcodedBroker := scanBannedKafkaShapes(t, path)

	if hasSkipGate {
		t.Errorf("%s: gates on os.Getenv(\"KAFKA_BROKERS\") — this fleet's CI integration job provisions Postgres only, so a skip-gated Kafka test silently skips in CI and proves nothing there; start a real broker via testcontainers-go/modules/kafka instead", path)
	}
	if hasHardcodedBroker {
		t.Errorf("%s: hardcodes localhost:9092 — a fresh CI runner has no broker at that address; start one via testcontainers-go/modules/kafka instead", path)
	}
	if !strings.Contains(content, "testcontainers-go/modules/kafka") {
		t.Errorf("%s: touches Kafka but does not import github.com/testcontainers/testcontainers-go/modules/kafka — Kafka-touching integration tests in this fleet must start their own broker via testcontainers, never assume/skip on an external one", path)
	}
}

// integrationTestTouchesKafka reports whether a test file's source imports
// this fleet's Kafka client (segmentio/kafka-go) or references one of the
// Kafka shapes that would make it a Kafka-touching test.
func integrationTestTouchesKafka(content string) bool {
	return strings.Contains(content, "segmentio/kafka-go") ||
		strings.Contains(content, "kafka.Reader") ||
		strings.Contains(content, "kafka.Writer") ||
		strings.Contains(content, "KAFKA_BROKERS")
}

// scanBannedKafkaShapes scans the file's non-comment lines for the two
// banned literal shapes: a skip gate on os.Getenv("KAFKA_BROKERS") and a
// hardcoded localhost:9092 broker address. Line-by-line so a comment that
// merely MENTIONS the banned shapes (e.g. explaining that a real broker
// via testcontainers is used specifically so the test needs no
// KAFKA_BROKERS/localhost:9092) doesn't false-positive the same way a
// commented-out `t.Skip` line shouldn't either.
func scanBannedKafkaShapes(t *testing.T, path string) (hasSkipGate, hasHardcodedBroker bool) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		if strings.Contains(line, `os.Getenv("KAFKA_BROKERS")`) {
			hasSkipGate = true
		}
		if strings.Contains(line, "localhost:9092") {
			hasHardcodedBroker = true
		}
	}
	f.Close()
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return hasSkipGate, hasHardcodedBroker
}
