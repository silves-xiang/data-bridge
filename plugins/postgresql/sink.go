package postgresql

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// pgxRows adapts pgx.Rows to implement sink.Rows (Close returns error).
type pgxRows struct {
	pgx.Rows
}

func (r *pgxRows) Close() error {
	r.Rows.Close()
	return nil
}

// pgExecutor wraps *pgxpool.Pool to implement sink.Executor.
type pgExecutor struct {
	pool *pgxpool.Pool
}

func (e *pgExecutor) Exec(ctx context.Context, query string, args ...any) error {
	_, err := e.pool.Exec(ctx, query, args...)
	return err
}

func (e *pgExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	rows, err := e.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{Rows: rows}, nil
}

var _ io.Closer = (*pgxRows)(nil)

// Sink implements sink.Sink for PostgreSQL.
type Sink struct {
	pool   *pgxpool.Pool
	config PGConnection
	exec   *pgExecutor
}

// Open establishes a PostgreSQL connection pool.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
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
	s.exec = &pgExecutor{pool: pool}
	return nil
}

// Close closes the PostgreSQL connection pool.
func (s *Sink) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Executor returns an executor for hooks to run SQL.
func (s *Sink) Executor() sink.Executor {
	return s.exec
}

// PrepareTarget runs before migration.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	// Create schemas if they don't exist.
	seenSchemas := map[string]bool{}
	for _, t := range tables {
		if t.Schema != "" && !seenSchemas[t.Schema] {
			seenSchemas[t.Schema] = true
			_, _ = s.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", t.Schema))
		}
	}
	return nil
}

// CleanupTarget runs after migration.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	// Run ANALYZE to update statistics.
	_, err := s.pool.Exec(ctx, "ANALYZE")
	return err
}

// CreateTable creates the target table in PostgreSQL.
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	var cols []string
	var pkCols []string

	for _, col := range table.Columns {
		mappedType := MapTargetType(col)

		colDef := fmt.Sprintf("%q %s", col.Name, mappedType)
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		if col.Default != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.Default)
		}
		cols = append(cols, colDef)

		if col.PrimaryKey {
			pkCols = append(pkCols, fmt.Sprintf("%q", col.Name))
		}
	}

	if len(pkCols) > 0 {
		cols = append(cols, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %q.%q (%s)",
		table.Schema, table.Name, strings.Join(cols, ", "))

	_, err := s.pool.Exec(ctx, createSQL)
	return err
}

// WriteBatch inserts a batch of rows using pgx CopyFrom for maximum performance.
// Falls back to multi-row INSERT with ON CONFLICT DO NOTHING for idempotency.
func (s *Sink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	colNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colNames[i] = col.Name
	}

	// Use COPY protocol for bulk insertion (fastest path).
	copyCount, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{table.Schema, table.Name},
		colNames,
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		// Fallback to INSERT ... ON CONFLICT DO NOTHING for idempotency.
		return s.insertBatch(ctx, table, rows)
	}

	return int(copyCount), nil
}

// insertBatch performs a multi-row INSERT ... ON CONFLICT DO NOTHING.
func (s *Sink) insertBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	colNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colNames[i] = fmt.Sprintf("%q", col.Name)
	}

	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(table.Columns))
	argIdx := 1

	for i := range rows {
		rowPlaceholders := make([]string, len(table.Columns))
		for j := range table.Columns {
			rowPlaceholders[j] = fmt.Sprintf("$%d", argIdx)
			argIdx++
			args = append(args, rows[i][j])
		}
		placeholders[i] = "(" + strings.Join(rowPlaceholders, ", ") + ")"
	}

	pkConflict := ""
	if len(table.PrimaryKey) > 0 {
		pkCols := make([]string, len(table.PrimaryKey))
		for i, pk := range table.PrimaryKey {
			pkCols[i] = fmt.Sprintf("%q", pk)
		}
		pkConflict = fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(pkCols, ", "))
	}

	query := fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES %s%s",
		table.Schema, table.Name,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
		pkConflict,
	)

	tag, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insert: %w", err)
	}

	return int(tag.RowsAffected()), nil
}
