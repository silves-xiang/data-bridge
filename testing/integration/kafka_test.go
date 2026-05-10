//go:build integration
// +build integration

package integration

import (
	"testing"

	_ "github.com/silves-xiang/data-bridge/plugins/kafka"
)

// TestKafkaToPostgres is skipped because Kafka in Docker requires the
// advertised listener to be set to the mapped host:port, which is only
// known after the container starts. This is a well-known limitation of
// Kafka + Docker testing.
//
// To test manually:
//   1. Start Kafka: docker run -p 9092:9092 bitnami/kafka:3.6
//   2. Create a topic and seed messages
//   3. Run databridge with a kafka source config
func TestKafkaToPostgres(t *testing.T) {
	t.Skip("Kafka integration test requires manual setup due to Docker networking limitations")
}
