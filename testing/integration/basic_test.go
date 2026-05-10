//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"
)

// TestBasicMySQLToPostgres tests a full migration with various data types.
func TestBasicMySQLToPostgres(t *testing.T) {
	env := SetupTestEnv(t)
	ctx := context.Background()
	defer env.Teardown(ctx)

	mysqlDB, err := sql.Open("mysql", env.MySQLDSN())
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	defer mysqlDB.Close()

	if err := mysqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}

	createSQL := `
	CREATE TABLE users (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(255),
		age INT,
		score DECIMAL(10,2),
		is_active TINYINT(1) DEFAULT 1,
		bio TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		birth_date DATE,
		metadata JSON,
		avatar BLOB
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := mysqlDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert test data row by row for clarity.
	insertRow := func(name, email interface{}, age, score interface{}, isActive interface{}, bio, createdAt, birthDate, metadata, avatar interface{}) {
		_, err := mysqlDB.ExecContext(ctx,
			`INSERT INTO users (name, email, age, score, is_active, bio, created_at, birth_date, metadata, avatar)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			name, email, age, score, isActive, bio, createdAt, birthDate, metadata, avatar,
		)
		if err != nil {
			t.Fatalf("insert row %v: %v", name, err)
		}
	}

	insertRow("Alice", "alice@example.com", 30, 95.50, 1, "Hello from Alice!", "2024-01-15 10:30:00", "1994-03-20", `{"lang":"en","verified":true}`, []byte("avatar1"))
	insertRow("Bob", "bob@test.org", nil, nil, 0, nil, "2024-06-01 14:00:00", nil, `{"lang":"fr"}`, nil)
	insertRow("Zhang San", "zhang@example.cn", 28, 88.88, 1, "unicode test", "2025-01-01 00:00:00", "1997-07-15", `{"emoji":"smile"}`, []byte("zhang-avatar"))
	insertRow("O'Brien", "obrien@pub.ie", 45, 100.00, 1, "quotes and things", "2023-12-31 23:59:59", "1979-11-01", `{"nested":{"key":"value"}}`, []byte("bonus-avatar-data"))
	insertRow("NULL User", nil, nil, nil, nil, nil, nil, nil, nil, nil)

	t.Log("inserted 5 test rows")

	cfg := &config.Config{
		Task:   config.TaskConfig{Name: "test-basic", Mode: "full"},
		Source: config.ConnectorConfig{Type: "mysql", Connection: map[string]any{
			"host": env.MySQLHost, "port": env.MySQLPort,
			"user": env.MySQLUser, "password": env.MySQLPassword, "database": env.MySQLDatabase,
		}},
		Sink: config.ConnectorConfig{Type: "postgresql", Connection: map[string]any{
			"host": env.PGHost, "port": env.PGPort,
			"user": env.PGUser, "password": env.PGPassword, "database": env.PGDatabase,
			"ssl_mode": "disable", "search_path": "public",
		}},
		Tables:        []config.TableConfig{{Source: "users", Target: "users"}},
		Parallelism:   1,
		ErrorHandling: config.ErrorConfig{Mode: "fail_fast", MaxRetries: 2},
		Checkpoint:    config.CheckpointConfig{Enabled: true, Dir: t.TempDir()},
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := p.Run(ctx); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	t.Log("migration done")

	// Verify in PostgreSQL.
	pgPool, err := pgxpool.New(ctx, env.PGDSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	var count int
	if err := pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("row count = %d, want 5", count)
	}

	// Spot-check Alice.
	var name string
	var email *string
	var age *int32
	if err := pgPool.QueryRow(ctx, "SELECT name, email, age FROM users WHERE name = 'Alice'").Scan(&name, &email, &age); err != nil {
		t.Fatalf("query Alice: %v", err)
	}
	if name != "Alice" {
		t.Errorf("name = %q", name)
	}
	if email == nil || *email != "alice@example.com" {
		t.Errorf("email = %v", email)
	}
	if age == nil || *age != 30 {
		t.Errorf("age = %v", age)
	}

	// Verify NULL user has nulls.
	var nullEmail *string
	if err := pgPool.QueryRow(ctx, "SELECT email FROM users WHERE name = 'NULL User'").Scan(&nullEmail); err != nil {
		t.Fatalf("query NULL User: %v", err)
	}
	if nullEmail != nil {
		t.Errorf("email should be nil, got %v", *nullEmail)
	}

	fmt.Println("basic migration test passed")
}

// TestEmptyTableMigration tests migrating a table with no rows.
func TestEmptyTableMigration(t *testing.T) {
	env := SetupTestEnv(t)
	ctx := context.Background()
	defer env.Teardown(ctx)

	mysqlDB, err := sql.Open("mysql", env.MySQLDSN())
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	defer mysqlDB.Close()

	if _, err := mysqlDB.ExecContext(ctx, "CREATE TABLE empty_table (id BIGINT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(100))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cfg := &config.Config{
		Task:   config.TaskConfig{Name: "test-empty", Mode: "full"},
		Source: config.ConnectorConfig{Type: "mysql", Connection: map[string]any{
			"host": env.MySQLHost, "port": env.MySQLPort,
			"user": env.MySQLUser, "password": env.MySQLPassword, "database": env.MySQLDatabase,
		}},
		Sink: config.ConnectorConfig{Type: "postgresql", Connection: map[string]any{
			"host": env.PGHost, "port": env.PGPort,
			"user": env.PGUser, "password": env.PGPassword, "database": env.PGDatabase,
			"ssl_mode": "disable", "search_path": "public",
		}},
		Parallelism:   1,
		ErrorHandling: config.ErrorConfig{Mode: "fail_fast", MaxRetries: 2},
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := p.Run(ctx); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	pgPool, err := pgxpool.New(ctx, env.PGDSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	var count int
	if err := pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM empty_table").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("empty table has %d rows, want 0", count)
	}

	fmt.Println("empty table test passed")
}
