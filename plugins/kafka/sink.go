package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/IBM/sarama"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// kafkaExecutor implements sink.Executor as a no-op for Kafka.
type kafkaExecutor struct{}

func (e *kafkaExecutor) Exec(ctx context.Context, query string, args ...any) error {
	return fmt.Errorf("kafka: Exec not supported")
}

func (e *kafkaExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	return nil, fmt.Errorf("kafka: Query not supported")
}

// KafkaSinkConfig holds additional sink parameters.
type KafkaSinkConfig struct {
	KeyColumn   string // column to use as message key
	ValueColumn string // column to use as message value
}

func parseSinkConfig(params map[string]any) KafkaSinkConfig {
	sc := KafkaSinkConfig{}
	if v, ok := params["key_column"].(string); ok {
		sc.KeyColumn = v
	}
	if v, ok := params["value_column"].(string); ok {
		sc.ValueColumn = v
	}
	return sc
}

// Sink implements sink.Sink for Kafka.
type Sink struct {
	producer sarama.SyncProducer
	config   KafkaConnection
	sinkCfg  KafkaSinkConfig
	exec     *kafkaExecutor
}

// Open establishes a Kafka connection.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)
	s.sinkCfg = parseSinkConfig(config)

	if len(s.config.Brokers) == 0 {
		return fmt.Errorf("kafka: brokers is required")
	}

	scfg := sarama.NewConfig()
	scfg.Producer.Return.Successes = true
	scfg.Producer.RequiredAcks = sarama.WaitForLocal
	scfg.Version = sarama.V2_1_0_0

	producer, err := sarama.NewSyncProducer(s.config.Brokers, scfg)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}

	s.producer = producer
	s.exec = &kafkaExecutor{}
	return nil
}

// Close closes the Kafka connection.
func (s *Sink) Close() error {
	if s.producer != nil {
		return s.producer.Close()
	}
	return nil
}

// Executor returns a no-op executor.
func (s *Sink) Executor() sink.Executor {
	return s.exec
}

// PrepareTarget is a no-op for Kafka.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	return nil
}

// CleanupTarget is a no-op for Kafka.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	return nil
}

// CreateTable is a no-op for Kafka (topics are auto-created if enabled).
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	return nil
}

// WriteBatch produces messages to a Kafka topic.
func (s *Sink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	colIdx := make(map[string]int)
	for i, col := range table.Columns {
		colIdx[col.Name] = i
	}

	messages := make([]*sarama.ProducerMessage, 0, len(rows))
	for _, row := range rows {
		key := s.getColumnValue(row, colIdx, s.sinkCfg.KeyColumn)
		value := s.getColumnValue(row, colIdx, s.sinkCfg.ValueColumn)
		// If no value column specified, serialize entire row as value.
		if s.sinkCfg.ValueColumn == "" {
			parts := make([]string, len(table.Columns))
			for i, col := range table.Columns {
				if i < len(row) && row[i] != nil {
					parts[i] = fmt.Sprintf("%s=%v", col.Name, row[i])
				}
			}
			value = strings.Join(parts, ",")
		}

		msg := &sarama.ProducerMessage{
			Topic: table.Name,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}
		messages = append(messages, msg)
	}

	err := s.producer.SendMessages(messages)
	if err != nil {
		if prodErrs, ok := err.(sarama.ProducerErrors); ok {
			sent := len(messages) - len(prodErrs)
			return sent, nil
		}
		return 0, fmt.Errorf("send: %w", err)
	}

	return len(rows), nil
}

// getColumnValue extracts a string value from a row by column name.
func (s *Sink) getColumnValue(row []any, colIdx map[string]int, colName string) string {
	if colName == "" {
		return ""
	}
	if idx, ok := colIdx[colName]; ok && idx < len(row) && row[idx] != nil {
		return fmt.Sprintf("%v", row[idx])
	}
	return ""
}
