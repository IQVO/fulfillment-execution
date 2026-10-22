//go:build integration

package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	adapter "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// A divert's event pair — WeightDiscrepancyDetected (the measurement) and
// PackageDiverted (the divert to problem-solve) — is published by the SAME
// use case in one Publish call. ADR 0053 promotes both to the integration
// topic for exception-management with byte-identical data. Against a REAL
// broker this must hold end to end: both messages land on
// warehouse.fulfillment.events under the exact full CloudEvents types,
// keyed and subject-stamped by the package id, on one partition (Hash
// balancer over the shared key), and nothing else rides along
// (LabelApplied/PackageSealed stay analytics-only). The payload bytes are
// pinned byte-for-byte against the analytics encoder by the unit wire test.
func TestPublisherDivertProducesPackageExceptionsOnIntegrationTopic(t *testing.T) {
	brokers := startKafkaContainer(t, "fulfillment-execution-kafka-itest-divert")
	const numPartitions = 8
	createTopicWithPartitions(t, brokers, adapter.Topic, numPartitions)

	publisher := adapter.NewPublisher(brokers, nil, nil, func() string { return "evt-divert-itest" })
	t.Cleanup(func() { _ = publisher.Close() })

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const pkg = "package-itest-divert"

	// The use case raises the pair together; the publisher gets one call.
	if err := publisher.Publish(context.Background(),
		shared.NewWeightDiscrepancyDetected(shared.PackageId(pkg), 1000, 1200, occurredAt),
		shared.NewPackageDiverted(shared.PackageId(pkg), occurredAt),
		shared.NewLabelApplied(shared.PackageId(pkg), occurredAt),
		shared.NewPackageSealed(shared.PackageId(pkg), occurredAt),
	); err != nil {
		t.Fatalf("publish divert pair: %v", err)
	}

	byKey, msgs := readAllPartitionsWithMessages(t, brokers, adapter.Topic, numPartitions)
	assertSingleKeyPartition(t, byKey, pkg, 2)
	assertCloudEventsOnTheWire(t, msgs,
		"com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected",
		"com.warehouse.wes.fulfillment-execution.package.PackageDiverted")

	const typeDiverted = "com.warehouse.wes.fulfillment-execution.package.PackageDiverted"
	const typeWeight = "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected"
	seen := map[string]int{}
	for _, m := range msgs {
		e, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("message is not a valid CloudEvent: %v (%s)", err, m.Value)
		}
		seen[e.Type()]++
	}
	if seen[typeDiverted] != 1 || seen[typeWeight] != 1 {
		t.Fatalf("occurrences on %s = %v, want exactly one %q and one %q", adapter.Topic, seen, typeDiverted, typeWeight)
	}
	if len(msgs) != 2 {
		t.Fatalf("read %d messages off %s, want exactly the 2 promoted ones (analytics-only events must not leak)", len(msgs), adapter.Topic)
	}
}
