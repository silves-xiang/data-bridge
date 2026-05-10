package kafka

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/IBM/sarama"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// KafkaConnection holds parsed connection parameters.
type KafkaConnection struct {
	Brokers []string
	Topics  []string
}

// parseConnection extracts Kafka connection params from config map.
func parseConnection(cfg map[string]any) KafkaConnection {
	c := KafkaConnection{}
	if addrs, ok := cfg["brokers"].([]any); ok {
		for _, a := range addrs {
			if s, ok := a.(string); ok {
				c.Brokers = append(c.Brokers, s)
			}
		}
	} else if addr, ok := cfg["brokers"].(string); ok {
		c.Brokers = strings.Split(addr, ",")
	}
	if topics, ok := cfg["topics"].([]any); ok {
		for _, t := range topics {
			if s, ok := t.(string); ok {
				c.Topics = append(c.Topics, s)
			}
		}
	}
	return c
}

// Source implements source.Source for Kafka.
type Source struct {
	client        sarama.Client
	config        KafkaConnection
	consumer      sarama.Consumer
	topicOffsets  map[string][]int64 // topic -> list of offsets (for pagination)
}

// Open establishes a Kafka connection.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	if len(s.config.Brokers) == 0 {
		return fmt.Errorf("kafka: brokers is required")
	}

	scfg := sarama.NewConfig()
	scfg.Consumer.Return.Errors = false
	scfg.Producer.Return.Successes = false
	scfg.Version = sarama.V2_1_0_0

	client, err := sarama.NewClient(s.config.Brokers, scfg)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		client.Close()
		return fmt.Errorf("kafka consumer: %w", err)
	}

	s.client = client
	s.consumer = consumer
	s.topicOffsets = make(map[string][]int64)
	return nil
}

// Close closes the Kafka connection.
func (s *Source) Close() error {
	if s.consumer != nil {
		s.consumer.Close()
	}
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Tables discovers topics and builds metadata.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	topics := s.config.Topics
	if len(topics) == 0 {
		var err error
		topics, err = s.client.Topics()
		if err != nil {
			return nil, fmt.Errorf("list topics: %w", err)
		}
		sort.Strings(topics)
		// Filter internal topics.
		filtered := make([]string, 0, len(topics))
		for _, t := range topics {
			if !strings.HasPrefix(t, "__") {
				filtered = append(filtered, t)
			}
		}
		topics = filtered
	}

	var tables []source.TableInfo
	for _, topic := range topics {
		info := s.topicInfo(topic)
		// Collect partition offsets for pagination.
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("partitions %s: %w", topic, err)
		}
		var offsets []int64
		for _, p := range partitions {
			off, err := s.client.GetOffset(topic, p, sarama.OffsetOldest)
			if err == nil {
				offsets = append(offsets, off)
			}
		}
		s.topicOffsets[topic] = offsets
		tables = append(tables, info)
	}
	return tables, nil
}

// topicInfo returns fixed schema for a Kafka topic.
func (s *Source) topicInfo(topic string) source.TableInfo {
	return source.TableInfo{
		Schema: "kafka",
		Name:   topic,
		Columns: []source.ColumnInfo{
			{Name: "_offset", OriginalType: "string", CommonType: int(schema.TypeString), PrimaryKey: true},
			{Name: "_partition", OriginalType: "string", CommonType: int(schema.TypeString)},
			{Name: "_key", OriginalType: "string", CommonType: int(schema.TypeString), Nullable: true},
			{Name: "_value", OriginalType: "string", CommonType: int(schema.TypeString), Nullable: true},
			{Name: "_timestamp", OriginalType: "string", CommonType: int(schema.TypeString)},
		},
		PrimaryKey: []string{"_offset"},
	}
}

// EstimateRowCount returns 0; counting is expensive in Kafka.
func (s *Source) EstimateRowCount(ctx context.Context, tableName string) (int64, error) {
	return 0, nil
}

// ReadBatch consumes messages from a topic partition starting at offset.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	batchSize := 1000
	partitions, err := s.client.Partitions(table.Name)
	if err != nil {
		return source.RowBatch{}, fmt.Errorf("partitions: %w", err)
	}

	batch := source.RowBatch{Offset: offset}

	totalCollected := 0
	for _, partition := range partitions {
		if totalCollected >= batchSize {
			break
		}

		// Get oldest offset and apply our pagination offset.
		oldest, err := s.client.GetOffset(table.Name, partition, sarama.OffsetOldest)
		if err != nil {
			continue
		}
		newest, err := s.client.GetOffset(table.Name, partition, sarama.OffsetNewest)
		if err != nil {
			continue
		}

		startOffset := oldest + int64(offset)
		if startOffset >= newest {
			continue
		}

		pc, err := s.consumer.ConsumePartition(table.Name, partition, startOffset)
		if err != nil {
			continue
		}

		collected := 0
		for msg := range pc.Messages() {
			if collected >= batchSize/totalCollected + 1 {
				break
			}
			row := []any{
				fmt.Sprintf("%d", msg.Offset),
				fmt.Sprintf("%d", msg.Partition),
				string(msg.Key),
				string(msg.Value),
				msg.Timestamp.Format("2006-01-02 15:04:05.999999999"),
			}
			batch.Rows = append(batch.Rows, row)
			collected++
			totalCollected++
		}
		pc.Close()
	}

	if totalCollected < batchSize {
		batch.IsLast = true
	}
	if len(batch.Rows) == 0 && offset > 0 {
		return source.RowBatch{}, io.EOF
	}

	return batch, nil
}
