package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// CHConnection holds parsed connection parameters.
type CHConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// parseConnection extracts ClickHouse connection params from config map.
func parseConnection(cfg map[string]any) CHConnection {
	c := CHConnection{Port: 9000}
	if v, ok := cfg["host"].(string); ok {
		c.Host = v
	}
	if v, ok := cfg["port"].(float64); ok {
		c.Port = int(v)
	} else if v, ok := cfg["port"].(int); ok {
		c.Port = v
	}
	if v, ok := cfg["user"].(string); ok {
		c.User = v
	}
	if v, ok := cfg["password"].(string); ok {
		c.Password = v
	}
	if v, ok := cfg["database"].(string); ok {
		c.Database = v
	}
	return c
}

// Source implements source.Source for ClickHouse.
type Source struct {
	db        *sql.DB
	config    CHConnection
	batchSize int
}

// Open establishes a ClickHouse connection.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)
	s.batchSize = 1000
	if bs, ok := config["batch_size"].(float64); ok && bs > 0 {
		s.batchSize = int(bs)
	} else if bs, ok := config["batch_size"].(int); ok && bs > 0 {
		s.batchSize = bs
	}

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s&read_timeout=30s",
		s.config.User, s.config.Password, s.config.Host, s.config.Port, s.config.Database)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	s.db = db
	return nil
}

// Close closes the ClickHouse connection.
func (s *Source) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Tables discovers all tables in the source database.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT name FROM system.tables WHERE database = ?", s.config.Database)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var tables []source.TableInfo
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		info, err := s.tableInfo(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", tableName, err)
		}
		tables = append(tables, info)
	}
	return tables, rows.Err()
}

// tableInfo returns metadata for a single table.
func (s *Source) tableInfo(ctx context.Context, tableName string) (source.TableInfo, error) {
	info := source.TableInfo{
		Schema: s.config.Database,
		Name:   tableName,
	}

	colRows, err := s.db.QueryContext(ctx,
		"SELECT name, type, is_in_primary_key FROM system.columns WHERE database = ? AND table = ? ORDER BY position",
		s.config.Database, tableName)
	if err != nil {
		return info, fmt.Errorf("describe table: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var colName, colType string
		var isPK uint8
		if err := colRows.Scan(&colName, &colType, &isPK); err != nil {
			return info, fmt.Errorf("scan column: %w", err)
		}

		nullable := strings.HasPrefix(colType, "Nullable(")
		commonType, _, _, _, err := MapSourceType(colType)
		if err != nil {
			commonType = schema.TypeText
		}

		col := source.ColumnInfo{
			Name:         colName,
			OriginalType: colType,
			CommonType:   int(commonType),
			Nullable:     nullable,
			PrimaryKey:   isPK == 1,
		}
		if col.PrimaryKey {
			info.PrimaryKey = append(info.PrimaryKey, col.Name)
		}
		info.Columns = append(info.Columns, col)
	}
	return info, colRows.Err()
}

// EstimateRowCount returns the approximate row count for a table.
func (s *Source) EstimateRowCount(ctx context.Context, tableName string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count() FROM `%s`.`%s`", s.config.Database, tableName),
	).Scan(&count)
	return count, err
}

// ReadBatch reads a page of rows using cursor-based pagination on the first PK column.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	batchSize := s.batchSize
	cursorIdx := cursorIndex(table)

	var query string
	if len(table.PrimaryKey) > 0 && cursorIdx >= 0 && offset > 0 {
		pkCol := table.PrimaryKey[0]
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE `%s` > %d ORDER BY `%s` LIMIT %d",
			s.config.Database, table.Name, pkCol, offset, pkCol, batchSize)
	} else if len(table.PrimaryKey) > 0 && cursorIdx >= 0 {
		pkCol := table.PrimaryKey[0]
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `%s` LIMIT %d",
			s.config.Database, table.Name, pkCol, batchSize)
	} else {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d",
			s.config.Database, table.Name, batchSize)
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return source.RowBatch{}, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return source.RowBatch{}, fmt.Errorf("columns: %w", err)
	}

	batch := source.RowBatch{Offset: offset}
	var lastCursor any

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return batch, fmt.Errorf("scan: %w", err)
		}

		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		if cursorIdx >= 0 && cursorIdx < len(values) {
			lastCursor = values[cursorIdx]
		}
		batch.Rows = append(batch.Rows, values)
	}

	if err := rows.Err(); err != nil {
		return batch, fmt.Errorf("rows iteration: %w", err)
	}

	if len(batch.Rows) < batchSize {
		batch.IsLast = true
	}
	if lastCursor != nil {
		batch.NextOffset = toUint64(lastCursor)
	}

	return batch, nil
}

// cursorIndex returns the column index of the first primary key, or -1.
func cursorIndex(table source.TableInfo) int {
	if len(table.PrimaryKey) == 0 {
		return -1
	}
	return columnIndex(table, table.PrimaryKey[0])
}

func columnIndex(table source.TableInfo, name string) int {
	for i, col := range table.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func toUint64(v any) uint64 {
	switch t := v.(type) {
	case int64:
		return uint64(t)
	case int32:
		return uint64(t)
	case float64:
		return uint64(t)
	case string:
		var n uint64
		fmt.Sscanf(t, "%d", &n)
		return n
	}
	return 0
}
