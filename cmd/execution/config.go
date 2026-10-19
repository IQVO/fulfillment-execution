package main

import (
	"errors"
	"fmt"
	"strings"
)

// Boot errors of the mandatory configuration. Each names its variable
// (the real-wiring ADR): the service runs on real Postgres and real Kafka,
// with no default and no degraded mode behind either.
var (
	errNoDatabase          = errors.New("DATABASE_URL is required: this context runs on real Postgres")
	errNoBrokers           = errors.New("KAFKA_BROKERS is required: this context runs on real Kafka")
	errNoWorkReleasedGroup = errors.New("WORK_RELEASED_CONSUMER_GROUP is required: the WorkReleased consumer always runs on real Kafka (a stable consumer group id for the warehouse.work-planning.events consumer)")
)

// bootConfig is the validated mandatory configuration of cmd/execution.
type bootConfig struct {
	databaseURL string
	// migrationsDatabaseURL is a DIRECT (non-pooled, session-mode) Postgres
	// connection string used ONLY for the golang-migrate startup step; it
	// falls back to databaseURL (see openStorage's doc comment and ADR-0031).
	migrationsDatabaseURL string
	// migrationsPath is the golang-migrate source directory (MIGRATIONS_PATH,
	// default "migrations", the directory the image ships).
	migrationsPath    string
	kafkaBrokers      []string
	workReleasedGroup string
	classification    classificationConfig
	interleaving      interleavingConfig
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

// checkEventPublisher validates EVENT_PUBLISHER. The only publisher is the
// Kafka-backed transactional outbox: unset and "kafka" are accepted (the
// variable is kept because the chart and warehouse-infra set it), "log" and
// anything else is a boot error naming the variable.
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

// loadBootConfig reads and validates the mandatory environment of
// cmd/execution. Every problem is reported at once, each naming its
// variable, so the process fails fast at boot, before it opens a connection
// or binds the listener.
func loadBootConfig(env func(string) string) (bootConfig, error) {
	databaseURL := env("DATABASE_URL")
	cfg := bootConfig{
		databaseURL:           databaseURL,
		migrationsDatabaseURL: databaseURL,
		migrationsPath:        "migrations",
		kafkaBrokers:          parseBrokers(env("KAFKA_BROKERS")),
		workReleasedGroup:     strings.TrimSpace(env("WORK_RELEASED_CONSUMER_GROUP")),
	}
	if v := env("MIGRATIONS_DATABASE_URL"); v != "" {
		cfg.migrationsDatabaseURL = v
	}
	if v := env("MIGRATIONS_PATH"); v != "" {
		cfg.migrationsPath = v
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errNoDatabase)
	}
	if len(cfg.kafkaBrokers) == 0 {
		errs = append(errs, errNoBrokers)
	}
	if cfg.workReleasedGroup == "" {
		errs = append(errs, errNoWorkReleasedGroup)
	}
	errs = append(errs, checkEventPublisher(env("EVENT_PUBLISHER")))
	var err error
	if cfg.classification, err = parseClassificationConfig(env("PRODUCT_CLASSIFICATION_MODE"), env("PRODUCT_CLASSIFICATION_CONSUMER_GROUP")); err != nil {
		errs = append(errs, err)
	}
	if cfg.interleaving, err = parseInterleavingConfig(env("TASK_INTERLEAVING_MODE"), env("TASK_INTERLEAVING_CONSUMER_GROUP")); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
