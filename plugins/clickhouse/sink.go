package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// chExecutor wraps *sql.DB to implement sink.Executor.
type chExecutor struct {
	db *sql.DB
}

func (e *chExecutor) Exec(ctx context.Context, query string, args ...any) error {
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *chExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

// Sink implements sink.Sink for ClickHouse.
type Sink struct {
	db     *sql.DB
	config CHConnection
	exec   *chExecutor
}

// Open establishes a ClickHouse connection.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s",
		s.config.User, s.config.Password, s.config.Host, s.config.Port, s.config.Database)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	s.db = db
	s.exec = &chExecutor{db: db}
	return nil
}

// Close closes the ClickHouse connection.
func (s *Sink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Executor returns an executor for hooks to run SQL.
func (s *Sink) Executor() sink.Executor {
	return s.exec
}

// PrepareTarget creates the database if it doesn't exist.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", s.config.Database))
	return err
}

// CleanupTarget is a no-op for ClickHouse.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	return nil
}

// CreateTable creates the target table with MergeTree engine.
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	var cols []string
	var pkCols []string

	for _, col := range table.Columns {
		mappedType := MapTargetType(col)
		colDef := fmt.Sprintf("`%s` %s", col.Name, mappedType)
		cols = append(cols, colDef)
		if col.PrimaryKey {
			pkCols = append(pkCols, "`"+col.Name+"`")
		}
	}

	orderBy := "tuple()"
	if len(pkCols) > 0 {
		orderBy = fmt.Sprintf("(%s)", strings.Join(pkCols, ", "))
	} else if len(table.Columns) > 0 {
		orderBy = fmt.Sprintf("`%s`", table.Columns[0].Name)
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) ENGINE = MergeTree ORDER BY %s",
		table.Schema, table.Name, strings.Join(cols, ", "), orderBy)

	_, err := s.db.ExecContext(ctx, createSQL)
	return err
}

// WriteBatch inserts a batch of rows.
func (s *Sink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	colNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colNames[i] = "`" + col.Name + "`"
	}

	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(table.Columns))

	for i := range rows {
		rowPlaceholders := make([]string, len(table.Columns))
		for j := range table.Columns {
			rowPlaceholders[j] = "?"
			args = append(args, rows[i][j])
		}
		placeholders[i] = "(" + strings.Join(rowPlaceholders, ", ") + ")"
	}

	query := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s",
		table.Schema, table.Name, strings.Join(colNames, ", "), strings.Join(placeholders, ", "))

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insert: %w", err)
	}

	return len(rows), nil
}
