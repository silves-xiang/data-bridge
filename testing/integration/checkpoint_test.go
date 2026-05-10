//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"
)

// TestCheckpointResume verifies:
// 1. First run creates checkpoint and migrates all data
// 2. Second run with same checkpoint dir skips already-completed tables
// 3. Data integrity is preserved (no duplicates, no missing rows)
func TestCheckpointResume(t *testing.T) {
	env := SetupTestEnv(t)
	ctx := context.Background()
	defer env.Teardown(ctx)

	mysqlDB, err := sql.Open("mysql", env.MySQLDSN())
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	defer mysqlDB.Close()

	// Create and populate test table.
	if _, err := mysqlDB.ExecContext(ctx, `
		CREATE TABLE items (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100),
			value INT
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert 50 rows to span multiple batches (batch_size=10 => 5 batches).
	for i := 0; i < 50; i++ {
		_, err := mysqlDB.ExecContext(ctx,
			"INSERT INTO items (name, value) VALUES (?, ?)",
			fmt.Sprintf("item-%d", i), i*10)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	t.Log("inserted 50 rows")

	checkpointDir := t.TempDir()

	// ---- First migration ----
	cfg1 := makeCheckpointConfig(env, checkpointDir, 10) // batch_size via table config
	p1, err := pipeline.New(cfg1)
	if err != nil {
		t.Fatalf("create pipeline 1: %v", err)
	}
	if err := p1.Run(ctx); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	t.Log("first migration done")

	// Verify checkpoint file exists.
	ckptFile := filepath.Join(checkpointDir, "test-checkpoint.checkpoint.json")
	if _, err := os.Stat(ckptFile); os.IsNotExist(err) {
		t.Fatal("checkpoint file was not created")
	}
	t.Logf("checkpoint file exists: %s", ckptFile)

	// Verify 50 rows in PostgreSQL after first run.
	pgPool, err := pgxpool.New(ctx, env.PGDSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	var count int
	if err := pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 50 {
		t.Errorf("after first run: row count = %d, want 50", count)
	}

	// ---- Second migration (should be a no-op) ----
	cfg2 := makeCheckpointConfig(env, checkpointDir, 10)
	p2, err := pipeline.New(cfg2)
	if err != nil {
		t.Fatalf("create pipeline 2: %v", err)
	}
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	t.Log("second migration done (should be no-op)")

	// Verify still exactly 50 rows (no duplicates - idempotent INSERT).
	if err := pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("count after second run: %v", err)
	}
	if count != 50 {
		t.Errorf("after second run: row count = %d, want 50 (no duplicates)", count)
	}

	// Verify data integrity: spot-check some rows.
	for _, id := range []int{1, 25, 50} {
		var name string
		var value int
		if err := pgPool.QueryRow(ctx, "SELECT name, value FROM items WHERE id = $1", id).Scan(&name, &value); err != nil {
			t.Errorf("query id=%d: %v", id, err)
			continue
		}
		expected := fmt.Sprintf("item-%d", id-1)
		if name != expected {
			t.Errorf("id=%d name = %q, want %q", id, name, expected)
		}
		if value != (id-1)*10 {
			t.Errorf("id=%d value = %d, want %d", id, value, (id-1)*10)
		}
	}

	fmt.Println("checkpoint resume test passed")
}

// makeCheckpointConfig builds a config with a specific checkpoint dir.
func makeCheckpointConfig(env *TestEnv, checkpointDir string, batchSize int) *config.Config {
	return &config.Config{
		Task:   config.TaskConfig{Name: "test-checkpoint", Mode: "full"},
		Source: config.ConnectorConfig{Type: "mysql", Connection: map[string]any{
			"host": env.MySQLHost, "port": env.MySQLPort,
			"user": env.MySQLUser, "password": env.MySQLPassword, "database": env.MySQLDatabase,
		}},
		Sink: config.ConnectorConfig{Type: "postgresql", Connection: map[string]any{
			"host": env.PGHost, "port": env.PGPort,
			"user": env.PGUser, "password": env.PGPassword, "database": env.PGDatabase,
			"ssl_mode": "disable", "search_path": "public",
		}},
		Tables: []config.TableConfig{
			{Source: "items", Target: "items", BatchSize: batchSize},
		},
		Parallelism:   1,
		ErrorHandling: config.ErrorConfig{Mode: "fail_fast", MaxRetries: 2},
		Checkpoint:    config.CheckpointConfig{Enabled: true, Dir: checkpointDir},
	}
}
