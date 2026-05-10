package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// mysqlExecutor wraps *sql.DB to implement sink.Executor.
type mysqlExecutor struct {
	db *sql.DB
}

func (e *mysqlExecutor) Exec(ctx context.Context, query string, args ...any) error {
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *mysqlExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

// Sink implements sink.Sink for MySQL.
type Sink struct {
	db     *sql.DB
	config MySQLConnection
	exec   *mysqlExecutor
}

// Open establishes a MySQL connection.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?charset=%s&parseTime=true&loc=Local&multiStatements=true",
		s.config.User, s.config.Password, s.config.Net,
		s.config.Host, s.config.Port, s.config.Database, s.config.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql open: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("mysql ping: %w", err)
	}

	s.db = db
	s.exec = &mysqlExecutor{db: db}
	return nil
}

// Close closes the MySQL connection.
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

// PrepareTarget runs before migration.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	// Disable foreign key checks for faster bulk loading.
	_, err := s.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0")
	return err
}

// CleanupTarget runs after migration.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1")
	return err
}

// CreateTable creates the target table.
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	var cols []string
	var pkCols []string

	for _, col := range table.Columns {
		// Map to MySQL type using the column's mapped type info.
		mappedType := MapTargetType(col)

		colDef := fmt.Sprintf("`%s` %s", col.Name, mappedType)
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		if col.AutoIncrement {
			colDef += " AUTO_INCREMENT"
		}
		if col.Default != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.Default)
		}
		cols = append(cols, colDef)

		if col.PrimaryKey {
			pkCols = append(pkCols, "`"+col.Name+"`")
		}
	}

	if len(pkCols) > 0 {
		cols = append(cols, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		table.Name, strings.Join(cols, ", "))

	_, err := s.db.ExecContext(ctx, createSQL)
	return err
}

// WriteBatch inserts a batch of rows using multi-row INSERT IGNORE (idempotent for resume).
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

	query := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s",
		table.Name, strings.Join(colNames, ", "), strings.Join(placeholders, ", "))

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insert: %w", err)
	}

	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// DB returns the underlying sql.DB for direct access.
func (s *Sink) DB() *sql.DB {
	return s.db
}
