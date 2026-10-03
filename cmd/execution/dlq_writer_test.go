package main

import "testing"

func TestNewDeadLetterWriter_FlushesPromptlyAndAutoCreates(t *testing.T) {
	w := newDeadLetterWriter([]string{"localhost:9092"})
	t.Cleanup(func() { _ = w.Close() })
	if w.BatchTimeout != dlqBatchTimeout {
		t.Fatalf("BatchTimeout = %v, want %v (kafka-go's 1s default caps dead-lettering at ~1 msg/s)", w.BatchTimeout, dlqBatchTimeout)
	}
	if !w.AllowAutoTopicCreation {
		t.Fatal("DLQ writer must auto-create the .dlq topic")
	}
}
