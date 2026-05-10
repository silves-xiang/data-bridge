package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// redisExecutor implements sink.Executor as a no-op for Redis.
type redisExecutor struct{}

func (e *redisExecutor) Exec(ctx context.Context, query string, args ...any) error {
	return fmt.Errorf("redis: Exec not supported")
}

func (e *redisExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	return nil, fmt.Errorf("redis: Query not supported")
}

// RedisSinkConfig holds additional Redis sink parameters.
type RedisSinkConfig struct {
	KeyPrefix string // prefix for key names
	KeyColumn string // column to use as the key (appended after prefix)
}

func parseSinkConfig(params map[string]any) RedisSinkConfig {
	sc := RedisSinkConfig{KeyPrefix: "row:"}
	if v, ok := params["key_prefix"].(string); ok {
		sc.KeyPrefix = v
	}
	if v, ok := params["key_column"].(string); ok {
		sc.KeyColumn = v
	}
	return sc
}

// Sink implements sink.Sink for Redis.
type Sink struct {
	client  *redis.Client
	config  RedisConnection
	sinkCfg RedisSinkConfig
	exec    *redisExecutor
}

// Open establishes a Redis connection.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)
	s.sinkCfg = parseSinkConfig(config)

	client := redis.NewClient(&redis.Options{
		Addr:     s.config.Addr,
		Password: s.config.Password,
		DB:       s.config.DB,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return fmt.Errorf("redis ping: %w", err)
	}

	s.client = client
	s.exec = &redisExecutor{}
	return nil
}

// Close closes the Redis connection.
func (s *Sink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Executor returns a no-op executor.
func (s *Sink) Executor() sink.Executor {
	return s.exec
}

// PrepareTarget is a no-op for Redis.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	return nil
}

// CleanupTarget is a no-op for Redis.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	return nil
}

// CreateTable is a no-op for the schemaless Redis.
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	return nil
}

// WriteBatch writes rows as Redis hashes using HSET.
func (s *Sink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	colIdx := make(map[string]int)
	for i, col := range table.Columns {
		colIdx[col.Name] = i
	}

	pipe := s.client.Pipeline()
	for _, row := range rows {
		key := s.buildKey(row, colIdx, table)
		fields := make(map[string]any)
		for colName, idx := range colIdx {
			if idx >= len(row) || row[idx] == nil {
				continue
			}
			if colName == s.sinkCfg.KeyColumn {
				continue
			}
			fields[colName] = fmt.Sprintf("%v", row[idx])
		}
		if len(fields) > 0 {
			pipe.HSet(ctx, key, fields)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		// Redis pipeline all-or-nothing; partial success not measurable.
		return 0, fmt.Errorf("pipeline exec: %w", err)
	}

	return len(rows), nil
}

// buildKey constructs the Redis key from the row data.
func (s *Sink) buildKey(row []any, colIdx map[string]int, table source.TableInfo) string {
	if s.sinkCfg.KeyColumn != "" {
		if idx, ok := colIdx[s.sinkCfg.KeyColumn]; ok && idx < len(row) && row[idx] != nil {
			return s.sinkCfg.KeyPrefix + fmt.Sprintf("%v", row[idx])
		}
	}
	// Fallback: use table name as prefix with index-based key.
	return s.sinkCfg.KeyPrefix + table.Name + ":" + fmt.Sprintf("%v", row[0])
}
