package main

import (
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// The DLQ writer must match the fleet's synchronous-writer settings
// (internal/adapters/outbound/kafka/writer_config.go): Hash balancer so the
// original key still picks the partition, RequireAll so a dead-lettered
// message is never acknowledged-but-lost, and a prompt flush.
func TestNewDeadLetterWriter_MatchesSyncWriterConfig(t *testing.T) {
	w := newDeadLetterWriter([]string{"localhost:9092"})
	t.Cleanup(func() { _ = w.Close() })

	if w.BatchTimeout != 10*time.Millisecond {
		t.Fatalf("BatchTimeout = %v, want 10ms (kafka-go's 1s default caps dead-lettering at ~1 msg/s)", w.BatchTimeout)
	}
	if w.RequiredAcks != kafkago.RequireAll {
		t.Fatalf("RequiredAcks = %v, want RequireAll (the default RequireNone silently loses messages)", w.RequiredAcks)
	}
	if _, ok := w.Balancer.(*kafkago.Hash); !ok {
		t.Fatalf("Balancer = %T, want *kafka.Hash so the message key decides the partition", w.Balancer)
	}
	if !w.AllowAutoTopicCreation {
		t.Fatal("DLQ writer must auto-create the .dlq topic")
	}
}
