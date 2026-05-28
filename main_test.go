package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// TestBuildProducerConfig — pure unit test. No Kafka required.
// Edit the assertions below as you tune BuildProducerConfig() to
// satisfy the lesson's reliability requirements.
func TestBuildProducerConfig(t *testing.T) {
	cfg := BuildProducerConfig()
	if cfg == nil {
		t.Fatal("BuildProducerConfig() returned nil")
	}
	if cfg.Version.IsAtLeast(sarama.V3_0_0_0) == false {
		t.Errorf("Version too low: %v — want ≥ 3.0.0", cfg.Version)
	}
	// TODO: add lesson-specific assertions per README.md, e.g.:
	//   if cfg.Producer.RequiredAcks != sarama.WaitForAll {
	//       t.Errorf("RequiredAcks: got %v, want WaitForAll", cfg.Producer.RequiredAcks)
	//   }
	//   if !cfg.Producer.Idempotent { t.Error("Idempotent must be true") }
}

// TestEnvDefaults — sanity check that the env-driven helpers work.
func TestEnvDefaults(t *testing.T) {
	brokers := Brokers()
	if len(brokers) == 0 {
		t.Fatal("Brokers() returned empty list")
	}
	topic := Topic()
	if !strings.HasPrefix(topic, "lesson-") && os.Getenv("KAFKA_TOPIC") == "" {
		t.Errorf("Topic() default = %q, expected prefix 'lesson-'", topic)
	}
}

// TestProduceConsume — integration test. Requires a running Kafka
// cluster (see docker-compose.yml). SKIPPED by default so CI green
// out of the box; remove t.Skip when your Produce/Consume work.
func TestProduceConsume(t *testing.T) {
	t.Skip("requires running Kafka — `docker compose up -d`, then remove this skip")

	brokers := Brokers()
	topic := Topic() + "-test"
	messages := []string{"msg-1", "msg-2", "msg-3"}

	part, off, err := Produce(brokers, topic, messages)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
	t.Logf("produced %d msgs → partition=%d offset=%d", len(messages), part, off)

	got, err := Consume(brokers, topic, len(messages), 10*time.Second)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(got) != len(messages) {
		t.Errorf("consumed %d, want %d", len(got), len(messages))
	}
	for i, m := range messages {
		if i < len(got) && got[i] != m {
			t.Errorf("got[%d] = %q, want %q", i, got[i], m)
		}
	}
}
