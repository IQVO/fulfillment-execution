package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Boot errors of the mandatory configuration. Each names its variable
// (the real-wiring ADR): cmd/a2a reads and writes the same Postgres as
// cmd/execution, and complete_task publishes through the transactional
// outbox that cmd/execution's relay drains onto real Kafka.
var (
	errNoDatabase = errors.New("DATABASE_URL is required: this context runs on real Postgres")
	errNoBrokers  = errors.New("KAFKA_BROKERS is required: this context runs on real Kafka")
	// errNoAgentURL: the Agent Card must advertise the absolute URL clients
	// dial (ADR-0059).
	errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR-0059), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")
)

// bootConfig is the validated mandatory configuration of cmd/a2a.
type bootConfig struct {
	databaseURL string
	// migrationsDatabaseURL is a DIRECT (non-pooled) Postgres connection
	// string used ONLY for the golang-migrate startup step; it falls back to
	// databaseURL (see buildStorage's doc comment and ADR-0031).
	migrationsDatabaseURL string
	kafkaBrokers          []string
	// agentURL is the absolute http(s) URL advertised on the Agent Card.
	agentURL string
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

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract - a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR-0059)", raw)
	}
	return nil
}

// loadBootConfig reads and validates the mandatory environment of cmd/a2a.
// Every problem is reported at once, each naming its variable, so the
// process fails fast at boot, before it opens a connection or binds the
// listener.
func loadBootConfig(env func(string) string) (bootConfig, error) {
	databaseURL := env("DATABASE_URL")
	cfg := bootConfig{
		databaseURL:           databaseURL,
		migrationsDatabaseURL: databaseURL,
		kafkaBrokers:          parseBrokers(env("KAFKA_BROKERS")),
		agentURL:              strings.TrimSpace(env("A2A_AGENT_URL")),
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
	errs = append(errs, checkEventPublisher(env("EVENT_PUBLISHER")), checkAgentURL(cfg.agentURL))
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
