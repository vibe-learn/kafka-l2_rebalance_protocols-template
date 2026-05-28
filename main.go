// Package main is the lesson `l2_rebalance_protocols` homework scaffold for Vibe Learn.
//
// Implement the functions below. The signatures and the test surface
// are fixed — the CI (.github/workflows/ci.yml) runs `go vet` and
// `go test ./...` against them. See README.md for the full task and
// acceptance criteria.
//
// sarama (IBM/sarama) is the chosen Kafka client. confluent-kafka-go
// requires a CGO build chain which complicates CI; sarama is pure-Go.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// ----- config -----

// envOr returns the env var for `key` if set, else `fallback`.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Brokers returns the bootstrap servers list. Default is the local
// 3-node KRaft cluster from docker-compose.yml.
func Brokers() []string {
	raw := envOr("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094")
	return strings.Split(raw, ",")
}

// Topic returns the topic this homework operates on. Override via
// env so tests can isolate runs.
func Topic() string {
	return envOr("KAFKA_TOPIC", "lesson-l2_rebalance_protocols")
}

// ----- TODO #1: producer config -----

// BuildProducerConfig assembles a *sarama.Config that satisfies THIS
// lesson's reliability/throughput requirements (see README.md).
//
// The CI test asserts specific fields (acks level, idempotence flag,
// max.in.flight, retries, etc.) — examine main_test.go for the
// contract and tune accordingly.
func BuildProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	// TODO: configure cfg.Producer.* per README.md task list.
	// Tip: cfg.Producer.RequiredAcks, cfg.Producer.Idempotent,
	//      cfg.Net.MaxOpenRequests, cfg.Producer.Retry.Max,
	//      cfg.Producer.Return.Successes (sync producers need this).
	return cfg
}

// ----- TODO #2: produce -----

// Produce sends `messages` to `topic` using a synchronous sarama
// producer built from BuildProducerConfig(). It must return the
// partition + offset of the LAST published record so the test can
// verify ordering.
//
// Hint: sarama.NewSyncProducer(brokers, BuildProducerConfig()).
func Produce(brokers []string, topic string, messages []string) (lastPartition int32, lastOffset int64, err error) {
	// TODO: implement
	return 0, 0, fmt.Errorf("Produce: not implemented")
}

// ----- TODO #3: consume -----

// Consume reads up to `expected` records from `topic` and returns
// their values in arrival order. Use a sarama.Consumer or a
// ConsumerGroup as the README directs. Block up to `timeout` then
// give up (so the test doesn't hang forever on an empty topic).
func Consume(brokers []string, topic string, expected int, timeout time.Duration) ([]string, error) {
	// TODO: implement
	return nil, fmt.Errorf("Consume: not implemented")
}

// ----- main entry -----

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("Vibe Learn — lesson %s scaffold up", "l2_rebalance_protocols")
	log.Printf("bootstrap: %v  topic: %s", Brokers(), Topic())
	log.Printf("Implement Produce / Consume, then `go test ./...`. README.md has the task.")

	// Graceful shutdown so `go run .` is interactive — Ctrl-C exits cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("shutdown signal received")
		cancel()
	}()
	<-ctx.Done()
}
