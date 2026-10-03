// Package architecture contains fitness tests that encode this module's
// hexagonal dependency rule as executable checks (the Go equivalent of
// ArchUnit), using github.com/arch-go/arch-go.
package architecture

import (
	"fmt"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

const modulePath = "github.com/claudioed/fulfillment-execution"

// TestHexagonalDependencyRules enforces the dependency rule documented in
// CLAUDE.md: dependencies point inward only, domain is pure, application
// depends only on domain, inbound and outbound adapters never depend on each
// other, and only cmd wires every layer together.
func TestHexagonalDependencyRules(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("domain has no internal dependencies except domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.domain.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{"**.domain.**"},
					},
				},
			},
		})

		assertPasses(t, result)
	})

	t.Run("application depends only on domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.application.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						// application's own subpackages (ports, usecases) may
						// depend on each other, and on domain, but nothing else.
						Internal: []string{"**.domain.**", "**.application.**"},
					},
				},
			},
		})

		assertPasses(t, result)
	})

	t.Run("inbound adapters do not depend on outbound adapters", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.inbound.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.outbound.**"},
					},
				},
			},
		})

		assertPasses(t, result)
	})

	t.Run("outbound adapters do not depend on inbound adapters", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.outbound.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.inbound.**"},
					},
				},
			},
		})

		assertPasses(t, result)
	})

	t.Run("only cmd wires every layer together", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					// Every package under internal/** (domain, application,
					// adapters) must never reach back into cmd/**; only cmd is
					// allowed to import from every layer to wire them together.
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.cmd.**"},
					},
				},
			},
		})

		assertPasses(t, result)
	})
}

// TestRepositoryPortImplementersFollowRepoNamingConvention is a bonus
// layer-convention check: every struct in this module that implements the
// TaskRepo port (defined in internal/application/ports) must have a simple
// name ending in "Repo" -- the naming convention this codebase already
// follows in both the memory and postgres outbound adapters.
func TestRepositoryPortImplementersFollowRepoNamingConvention(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
		NamingRules: []*configuration.NamingRule{
			{
				Package: "**.internal.**",
				InterfaceImplementationNamingRule: &configuration.InterfaceImplementationRule{
					StructsThatImplement:           "TaskRepo",
					ShouldHaveSimpleNameEndingWith: stringPtr("Repo"),
				},
			},
		},
	})

	assertPasses(t, result)
}

func assertPasses(t *testing.T, result *archgo.Result) {
	t.Helper()

	if !result.Pass {
		t.Fatalf("%s", archViolation("dependency", "architecture", describeViolations(result)))
	}
}

// describeViolations renders every failing arch-go rule in result into a
// human-readable block, delegating to one walker per rule kind.
func describeViolations(result *archgo.Result) string {
	var b strings.Builder
	describeDependenciesViolations(&b, result)
	describeNamingViolations(&b, result)
	return b.String()
}

// describeDependenciesViolations appends every failing dependency rule,
// with each rule's failing per-package verifications, to b.
func describeDependenciesViolations(b *strings.Builder, result *archgo.Result) {
	if result.DependenciesRuleResult == nil {
		return
	}
	for _, r := range result.DependenciesRuleResult.Results {
		if r.Passes {
			continue
		}

		fmt.Fprintf(b, "%s\n", r.Description)

		for _, v := range r.Verifications {
			if v.Passes {
				continue
			}

			fmt.Fprintf(b, "  package %s:\n", v.Package)

			for _, d := range v.Details {
				fmt.Fprintf(b, "    - %s\n", d)
			}
		}
	}
}

// describeNamingViolations appends every failing naming rule, with each
// rule's failing per-package verifications, to b. arch-go models
// dependency and naming results as distinct internal types that share the
// same shape, hence the parallel walker.
func describeNamingViolations(b *strings.Builder, result *archgo.Result) {
	if result.NamingRuleResult == nil {
		return
	}
	for _, r := range result.NamingRuleResult.Results {
		if r.Passes {
			continue
		}

		fmt.Fprintf(b, "%s\n", r.Description)

		for _, v := range r.Verifications {
			if v.Passes {
				continue
			}

			fmt.Fprintf(b, "  package %s:\n", v.Package)

			for _, d := range v.Details {
				fmt.Fprintf(b, "    - %s\n", d)
			}
		}
	}
}

func stringPtr(s string) *string {
	return &s
}
