package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/silves-xiang/data-bridge/pkg/source"
)

// Source implements source.Source for MySQL.
type Source struct {
	db     *sql.DB
	config MySQLConnection
	// tables holds the configured table list with filters applied.
	tables []source.TableInfo
}

// MySQLConnection holds parsed connection parameters.
type MySQLConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Charset  string
	Net      string
}

// parseConnection extracts MySQL connection params from the config map.
func parseConnection(cfg map[string]any) MySQLConnection {
	c := MySQLConnection{
		Charset: "utf8mb4",
		Net:     "tcp",
	}
	if v, ok := cfg["host"].(string); ok {
		c.Host = v
	}
	if v, ok := cfg["port"].(float64); ok { // YAML numbers are float64
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
	if v, ok := cfg["charset"].(string); ok {
		c.Charset = v
	}
	if v, ok := cfg["net"].(string); ok {
		c.Net = v
	}
	return c
}

// Open establishes a MySQL connection.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		s.config.User, s.config.Password, s.config.Net,
		s.config.Host, s.config.Port, s.config.Database, s.config.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql open: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("mysql ping: %w", err)
	}

	s.db = db
	return nil
}

// Close closes the MySQL connection.
func (s *Source) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Tables discovers all tables in the source database.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	rows, err := s.db.QueryContext(ctx, "SHOW TABLES")
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

	s.tables = tables
	return tables, rows.Err()
}

// tableInfo returns metadata for a single table.
func (s *Source) tableInfo(ctx context.Context, tableName string) (source.TableInfo, error) {
	info := source.TableInfo{
		Schema: s.config.Database,
		Name:   tableName,
	}

	// Get columns.
	colRows, err := s.db.QueryContext(ctx, "SHOW COLUMNS FROM `"+tableName+"`")
	if err != nil {
		return info, fmt.Errorf("describe table: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var field, colType, isNull, key, defaultVal, extra sql.NullString
		if err := colRows.Scan(&field, &colType, &isNull, &key, &defaultVal, &extra); err != nil {
			return info, fmt.Errorf("scan column: %w", err)
		}

		commonType, length, precision, scale, err := MapSourceType(colType.String)
		if err != nil {
			// Log warning, default to text.
			commonType = 19 // TypeText
		}

		col := source.ColumnInfo{
			Name:         field.String,
			OriginalType: colType.String,
			CommonType:   int(commonType),
			Nullable:     isNull.String == "YES",
			Length:       length,
			Precision:    precision,
			Scale:        scale,
			AutoIncrement: strings.Contains(extra.String, "auto_increment"),
			PrimaryKey:   key.String == "PRI",
			Default:      nil,
		}
		if defaultVal.Valid {
			s := defaultVal.String
			col.Default = &s
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
		"SELECT COUNT(*) FROM `"+tableName+"`").Scan(&count)
	return count, err
}

// ReadBatch reads a page of rows using cursor-based or offset-based pagination.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	batchSize := 1000

	// Use cursor-based pagination if there's a single auto-increment PK.
	var cursorCol *source.ColumnInfo
	if len(table.PrimaryKey) == 1 {
		for i := range table.Columns {
			if table.Columns[i].PrimaryKey && table.Columns[i].AutoIncrement {
				cursorCol = &table.Columns[i]
				break
			}
		}
	}

	var query string
	if cursorCol != nil {
		query = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` > %d ORDER BY `%s` LIMIT %d",
			table.Name, cursorCol.Name, offset, cursorCol.Name, batchSize)
	} else {
		query = fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d",
			table.Name, batchSize, offset*uint64(batchSize))
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

	for rows.Next() {
		// Create scan targets.
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return batch, fmt.Errorf("scan: %w", err)
		}

		// Normalize byte slices to strings for portability.
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		batch.Rows = append(batch.Rows, values)
	}

	if err := rows.Err(); err != nil {
		return batch, fmt.Errorf("rows iteration: %w", err)
	}

	// Determine if this is the last batch.
	if len(batch.Rows) < batchSize {
		batch.IsLast = true
	}

	return batch, nil
}

// DB returns the underlying sql.DB for direct access (used by hooks, etc.).
func (s *Source) DB() *sql.DB {
	return s.db
}
