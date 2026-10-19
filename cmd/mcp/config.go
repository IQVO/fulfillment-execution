package main

import (
	"errors"
	"fmt"
	"strings"
)

// Boot errors of the mandatory configuration. Each names its variable
// (the real-wiring ADR): cmd/mcp reads and writes the same Postgres as
// cmd/execution, and complete_task publishes through the transactional
// outbox that cmd/execution's relay drains onto real Kafka.
var (
	errNoDatabase = errors.New("DATABASE_URL is required: this context runs on real Postgres")
	errNoBrokers  = errors.New("KAFKA_BROKERS is required: this context runs on real Kafka")
)

// bootConfig is the validated mandatory configuration of cmd/mcp.
type bootConfig struct {
	databaseURL string
	// migrationsDatabaseURL is a DIRECT (non-pooled) Postgres connection
	// string used ONLY for the golang-migrate startup step; it falls back to
	// databaseURL (see buildStorage's doc comment and ADR-0031).
	migrationsDatabaseURL string
	kafkaBrokers          []string
}

// parseBrokers splits KAFKA_BROKERS (comma separated), trimming blanks. It
// returns nil when nothing is configured: there is no localhost default.
func parseBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// checkEventPublisher validates EVENT_PUBLISHER: unset and "kafka" are
// accepted (the variable is kept because the chart and warehouse-infra set
// it), "log" and anything else is a boot error naming the variable.
func checkEventPublisher(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "kafka") {
		return nil
	}
	if strings.EqualFold(raw, "log") {
		return errors.New("EVENT_PUBLISHER=log is not supported: this context publishes to real Kafka")
	}
	return fmt.Errorf("unknown EVENT_PUBLISHER %q: this context publishes to real Kafka (want kafka)", raw)
}

// loadBootConfig reads and validates the mandatory environment of cmd/mcp.
// Every problem is reported at once, each naming its variable, so the
// process fails fast at boot, before it opens a connection or binds the
// listener.
func loadBootConfig(env func(string) string) (bootConfig, error) {
	databaseURL := env("DATABASE_URL")
	cfg := bootConfig{
		databaseURL:           databaseURL,
		migrationsDatabaseURL: databaseURL,
		kafkaBrokers:          parseBrokers(env("KAFKA_BROKERS")),
	}
	if v := env("MIGRATIONS_DATABASE_URL"); v != "" {
		cfg.migrationsDatabaseURL = v
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errNoDatabase)
	}
	if len(cfg.kafkaBrokers) == 0 {
		errs = append(errs, errNoBrokers)
	}
	errs = append(errs, checkEventPublisher(env("EVENT_PUBLISHER")))
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
