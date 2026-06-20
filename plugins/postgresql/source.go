package postgresql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// Source implements source.Source for PostgreSQL.
type Source struct {
	pool   *pgxpool.Pool
	config PGConnection
}

// PGConnection holds parsed connection parameters.
type PGConnection struct {
	Host       string
	Port       int
	User       string
	Password   string
	Database   string
	SSLMode    string
	SearchPath string
}

// parseConnection extracts PostgreSQL connection params from config map.
func parseConnection(cfg map[string]any) PGConnection {
	c := PGConnection{
		SSLMode:    "disable",
		SearchPath: "public",
		Port:       5432,
	}
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
	if v, ok := cfg["ssl_mode"].(string); ok {
		c.SSLMode = v
	}
	if v, ok := cfg["search_path"].(string); ok {
		c.SearchPath = v
	}
	return c
}

// Open establishes a PostgreSQL connection pool.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s search_path=%s",
		s.config.Host, s.config.Port, s.config.User, s.config.Password,
		s.config.Database, s.config.SSLMode, s.config.SearchPath,
	)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("pgxpool new: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("pgx ping: %w", err)
	}

	s.pool = pool
	return nil
}

// Close closes the PostgreSQL connection pool.
func (s *Source) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Tables discovers all tables in the source database.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name`

	rows, err := s.pool.Query(ctx, query, s.config.SearchPath)
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
		Schema: s.config.SearchPath,
		Name:   tableName,
	}

	query := `
		SELECT
			c.column_name,
			c.data_type,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.is_nullable,
			c.column_default,
			CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_pk,
			c.is_identity
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT ku.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage ku
				ON tc.constraint_name = ku.constraint_name
				AND tc.table_schema = ku.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
				AND tc.table_schema = $1
				AND tc.table_name = $2
		) pk ON c.column_name = pk.column_name
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position`

	colRows, err := s.pool.Query(ctx, query, s.config.SearchPath, tableName)
	if err != nil {
		return info, fmt.Errorf("query columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var (
			colName, dataType, isNullable, colDefault, isIdentity string
			charMaxLen, numPrecision, numScale                    *int
			isPK                                                   bool
		)
		if err := colRows.Scan(&colName, &dataType, &charMaxLen, &numPrecision, &numScale,
			&isNullable, &colDefault, &isPK, &isIdentity); err != nil {
			return info, fmt.Errorf("scan column: %w", err)
		}

		var fullType string
		if charMaxLen != nil && *charMaxLen > 0 {
			fullType = fmt.Sprintf("%s(%d)", dataType, *charMaxLen)
		} else if numPrecision != nil && *numPrecision > 0 {
			if numScale != nil && *numScale > 0 {
				fullType = fmt.Sprintf("%s(%d,%d)", dataType, *numPrecision, *numScale)
			} else {
				fullType = fmt.Sprintf("%s(%d)", dataType, *numPrecision)
			}
		} else {
			fullType = dataType
		}

		commonType, _, precision, scale, err := MapSourceType(fullType)
		if err != nil {
			commonType = schema.TypeText
		}

		length := 0
		if charMaxLen != nil {
			length = *charMaxLen
		}

		autoIncrement := isIdentity == "YES" || strings.Contains(colDefault, "nextval")

		col := source.ColumnInfo{
			Name:          colName,
			OriginalType:  fullType,
			CommonType:    int(commonType),
			Nullable:      isNullable == "YES",
			Length:        length,
			Precision:     precision,
			Scale:         scale,
			AutoIncrement: autoIncrement,
			PrimaryKey:    isPK,
			Default:       nil,
		}
		if colDefault != "" && !strings.Contains(colDefault, "nextval") {
			s := colDefault
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
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %q.%q", s.config.SearchPath, tableName),
	).Scan(&count)
	return count, err
}

// ReadBatch reads a page of rows using cursor-based or offset-based pagination.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	batchSize := 1000

	// Use cursor-based if single auto-increment PK.
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
		query = fmt.Sprintf("SELECT * FROM %q.%q WHERE %q > %d ORDER BY %q LIMIT %d",
			table.Schema, table.Name, cursorCol.Name, offset, cursorCol.Name, batchSize)
	} else {
		query = fmt.Sprintf("SELECT * FROM %q.%q LIMIT %d OFFSET %d",
			table.Schema, table.Name, batchSize, offset*uint64(batchSize))
	}

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return source.RowBatch{}, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	batch := source.RowBatch{Offset: offset}
	var lastCursor any

	// Find the cursor column index for NextOffset tracking.
	cursorIdx := -1
	if cursorCol != nil {
		for i, col := range table.Columns {
			if col.Name == cursorCol.Name {
				cursorIdx = i
				break
			}
		}
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return batch, fmt.Errorf("values: %w", err)
		}

		// Normalize values for portability.
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
			// Convert time.Time to string for universal compatibility.
			if t, ok := v.(time.Time); ok {
				values[i] = t.Format("2006-01-02 15:04:05.999999")
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

	// Set NextOffset for cursor-based pagination.
	if cursorCol != nil && lastCursor != nil {
		batch.NextOffset = anyToUint64(lastCursor)
	} else {
		batch.NextOffset = offset + 1
	}

	return batch, nil
}

func anyToUint64(v any) uint64 {
	switch t := v.(type) {
	case int64:
		return uint64(t)
	case int32:
		return uint64(t)
	case float64:
		return uint64(t)
	case string:
		for _, c := range t {
			if c < '0' || c > '9' {
				return 0
			}
		}
		var n uint64
		fmt.Sscanf(t, "%d", &n)
		return n
	}
	return 0
}
