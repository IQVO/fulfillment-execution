// Command prektopics pre-creates every Kafka topic the fulfillment-execution
// service writes to or consumes from, so a cold broker never answers the
// outbox relay or a consumer with [3] Unknown Topic Or Partition while the
// broker's auto-creation is still electing a leader. scripts/contract-test.sh
// runs it before booting the service; it is idempotent (an existing topic is
// skipped) and waits for every topic to have a leader before returning.
//
// Usage: KAFKA_BROKERS=localhost:9092 go run ./scripts/prektopics
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// topics is every topic cmd/execution writes to or reads from: the outbox
// relay's two publish topics and the three consumer topics (the pack-level
// view consumer also reads warehouse.product-master.events).
var topics = []string{
	"warehouse.fulfillment.events",
	"warehouse.fulfillment.analytics",
	"warehouse.work-planning.events",
	"warehouse.task-interleaving.events",
	"warehouse.product-master.events",
}

func main() {
	broker := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")[0]
	if broker == "" {
		fmt.Fprintln(os.Stderr, "KAFKA_BROKERS is not set")
		os.Exit(1)
	}
	if err := run(broker); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(broker string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("dial kafka %s: %w", broker, err)
	}
	defer func() { _ = conn.Close() }()
	for _, topic := range topics {
		if err := ensureTopic(conn, topic); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "topic %s ready\n", topic)
	}
	return nil
}

// ensureTopic creates topic (an existing topic is fine) and waits until it
// has a leader, so a writer never meets [3] Unknown Topic Or Partition.
func ensureTopic(conn *kafkago.Conn, topic string) error {
	err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	var kerr kafkago.Error
	exists := errors.As(err, &kerr) && kerr == kafkago.TopicAlreadyExists
	if err != nil && !exists {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		parts, rerr := conn.ReadPartitions(topic)
		if rerr == nil && len(parts) == 1 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s has no leader after 20s: %w", topic, rerr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
