package redis

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/redis/go-redis/v9"
	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// RedisConnection holds parsed connection parameters.
type RedisConnection struct {
	Addr     string
	Password string
	DB       int
	// KeyPatterns maps table names to key patterns (e.g., "users" -> "user:*").
	KeyPatterns map[string]string
}

// parseConnection extracts Redis connection params from config map.
func parseConnection(cfg map[string]any) RedisConnection {
	c := RedisConnection{
		Addr: "localhost:6379",
		DB:   0,
	}
	if v, ok := cfg["addr"].(string); ok {
		c.Addr = v
	}
	if v, ok := cfg["password"].(string); ok {
		c.Password = v
	}
	if v, ok := cfg["db"].(float64); ok {
		c.DB = int(v)
	} else if v, ok := cfg["db"].(int); ok {
		c.DB = v
	}
	if patterns, ok := cfg["key_patterns"].(map[string]any); ok {
		c.KeyPatterns = make(map[string]string)
		for table, pattern := range patterns {
			if s, ok := pattern.(string); ok {
				c.KeyPatterns[table] = s
			}
		}
	}
	return c
}

// Source implements source.Source for Redis.
type Source struct {
	client  *redis.Client
	config  RedisConnection
	keyMeta map[string][]string // table -> list of matching keys
}

// Open establishes a Redis connection.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

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
	s.keyMeta = make(map[string][]string)
	return nil
}

// Close closes the Redis connection.
func (s *Source) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Tables discovers tables from configured key patterns.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	if len(s.config.KeyPatterns) == 0 {
		return nil, fmt.Errorf("redis: key_patterns is required (e.g., {\"users\": \"user:*\"})")
	}

	var tables []source.TableInfo
	for tableName, pattern := range s.config.KeyPatterns {
		// Scan all matching keys.
		var keys []string
		var cursor uint64
		for {
			var batch []string
			var err error
			batch, cursor, err = s.client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				return nil, fmt.Errorf("scan %s: %w", pattern, err)
			}
			keys = append(keys, batch...)
			if cursor == 0 {
				break
			}
		}
		sort.Strings(keys)
		s.keyMeta[tableName] = keys

		info, err := s.tableInfo(ctx, tableName, keys)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", tableName, err)
		}
		tables = append(tables, info)
	}
	return tables, nil
}

// tableInfo samples the first key to infer hash field names.
func (s *Source) tableInfo(ctx context.Context, tableName string, keys []string) (source.TableInfo, error) {
	info := source.TableInfo{
		Schema: fmt.Sprintf("db%d", s.config.DB),
		Name:   tableName,
	}

	if len(keys) == 0 {
		return info, nil
	}

	// Sample first key to get field names.
	fields, err := s.client.HGetAll(ctx, keys[0]).Result()
	if err != nil {
		return info, fmt.Errorf("hgetall sample: %w", err)
	}

	// Add _key column first.
	info.Columns = append(info.Columns, source.ColumnInfo{
		Name:         "_key",
		OriginalType: "string",
		CommonType:   int(schema.TypeString),
		PrimaryKey:   true,
	})
	info.PrimaryKey = []string{"_key"}

	for field := range fields {
		info.Columns = append(info.Columns, source.ColumnInfo{
			Name:         field,
			OriginalType: "string",
			CommonType:   int(schema.TypeString),
			Nullable:     true,
		})
	}

	return info, nil
}

// EstimateRowCount returns the count of matching keys.
func (s *Source) EstimateRowCount(ctx context.Context, tableName string) (int64, error) {
	keys := s.keyMeta[tableName]
	return int64(len(keys)), nil
}

// ReadBatch reads a page of hash keys.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	batchSize := 1000
	keys := s.keyMeta[table.Name]

	start := int(offset) * batchSize
	if start >= len(keys) {
		return source.RowBatch{}, io.EOF
	}

	end := start + batchSize
	if end > len(keys) {
		end = len(keys)
	}

	batchKeys := keys[start:end]
	batch := source.RowBatch{Offset: offset}

	for _, key := range batchKeys {
		fields, err := s.client.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}

		row := make([]any, len(table.Columns))
		// First column is always _key.
		row[0] = key

		colIdx := make(map[string]int)
		for i, col := range table.Columns {
			colIdx[col.Name] = i
		}

		for field, val := range fields {
			if idx, ok := colIdx[field]; ok {
				row[idx] = val
			}
		}
		batch.Rows = append(batch.Rows, row)
	}

	if end >= len(keys) {
		batch.IsLast = true
	}

	return batch, nil
}
