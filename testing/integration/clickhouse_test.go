//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"

	_ "github.com/silves-xiang/data-bridge/plugins/clickhouse"
	_ "github.com/silves-xiang/data-bridge/plugins/postgresql"
)

func setupClickHouse(t *testing.T) (host string, port int, teardown func()) {
	t.Helper()
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24-alpine",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_DB":       "testdb",
			"CLICKHOUSE_USER":     "testuser",
			"CLICKHOUSE_PASSWORD": "testpass",
		},
		WaitingFor: wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(2 * time.Minute),
	}

	ctn, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}

	h, _ := ctn.Host(ctx)
	p, _ := ctn.MappedPort(ctx, "9000/tcp")
	portNum, _ := strconv.Atoi(p.Port())

	return h, portNum, func() { ctn.Terminate(ctx) }
}

func TestClickHouseToPostgres(t *testing.T) {
	pgEnv := SetupTestEnv(t)
	ctx := context.Background()
	defer pgEnv.Teardown(ctx)

	chHost, chPort, chTeardown := setupClickHouse(t)
	defer chTeardown()

	t.Logf("ClickHouse: %s:%d", chHost, chPort)

	// Seed data in ClickHouse directly.
	chDSN := fmt.Sprintf("clickhouse://testuser:testpass@%s:%d/testdb?dial_timeout=10s", chHost, chPort)
	chDB, err := sql.Open("clickhouse", chDSN)
	if err != nil {
		t.Fatalf("connect clickhouse: %v", err)
	}
	defer chDB.Close()

	createSQL := `CREATE TABLE test_sensors (
		sensor_id Int32,
		name String,
		value Float64,
		active Bool,
		created_at DateTime
	) ENGINE = MergeTree ORDER BY sensor_id`
	if _, err := chDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert test rows.
	insertSQL := `INSERT INTO test_sensors (sensor_id, name, value, active, created_at) VALUES (?, ?, ?, ?, ?)`
	for i := 0; i < 5; i++ {
		_, err := chDB.ExecContext(ctx, insertSQL, int32(i+1),
			fmt.Sprintf("sensor-%d", i), 22.5+float64(i), i%2 == 0, time.Now())
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	t.Log("seeded 5 rows into ClickHouse")

	// Migrate ClickHouse → PostgreSQL.
	cfg := &config.Config{
		Task: config.TaskConfig{Name: "test-ch-to-pg", Mode: "full"},
		Source: config.ConnectorConfig{Type: "clickhouse", Connection: map[string]any{
			"host": chHost, "port": chPort, "user": "testuser", "password": "testpass", "database": "testdb",
		}},
		Sink: config.ConnectorConfig{Type: "postgresql", Connection: map[string]any{
			"host": pgEnv.PGHost, "port": pgEnv.PGPort, "user": pgEnv.PGUser,
			"password": pgEnv.PGPassword, "database": pgEnv.PGDatabase, "ssl_mode": "disable", "search_path": "public",
		}},
		Parallelism:   1,
		ErrorHandling: config.ErrorConfig{Mode: "fail_fast", MaxRetries: 2},
		Checkpoint:    config.CheckpointConfig{Enabled: false},
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := p.Run(ctx); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	// Verify in PostgreSQL.
	pgPool, err := pgxpool.New(ctx, pgEnv.PGDSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	var count int
	if err := pgPool.QueryRow(ctx, `SELECT COUNT(*) FROM "test_sensors"`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("row count = %d, want 5", count)
	}

	var name string
	if err := pgPool.QueryRow(ctx, `SELECT "name" FROM "test_sensors" WHERE "sensor_id" = '1'`).Scan(&name); err != nil {
		t.Fatalf("query: %v", err)
	}
	if name != "sensor-0" {
		t.Errorf("name = %q, want sensor-0", name)
	}

	fmt.Println("clickhouse → postgres test passed")
}

// TestClickHousePagination verifies cursor-based pagination with multiple batches.
func TestClickHousePagination(t *testing.T) {
	pgEnv := SetupTestEnv(t)
	ctx := context.Background()
	defer pgEnv.Teardown(ctx)

	chHost, chPort, chTeardown := setupClickHouse(t)
	defer chTeardown()

	chDSN := fmt.Sprintf("clickhouse://testuser:testpass@%s:%d/testdb?dial_timeout=10s", chHost, chPort)
	chDB, err := sql.Open("clickhouse", chDSN)
	if err != nil {
		t.Fatalf("connect clickhouse: %v", err)
	}
	defer chDB.Close()

	// Create table with PK.
	createSQL := `CREATE TABLE pagination_test (
		id Int32,
		val String
	) ENGINE = MergeTree ORDER BY id`
	if _, err := chDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert 12 rows — will need 3 batches at batch_size=5.
	for i := 0; i < 12; i++ {
		_, err := chDB.ExecContext(ctx,
			"INSERT INTO pagination_test (id, val) VALUES (?, ?)",
			int32(i+1), fmt.Sprintf("value-%d", i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	t.Log("seeded 12 rows into ClickHouse")

	cfg := &config.Config{
		Task: config.TaskConfig{Name: "test-ch-pagination", Mode: "full"},
		Source: config.ConnectorConfig{Type: "clickhouse", Connection: map[string]any{
			"host": chHost, "port": chPort, "user": "testuser", "password": "testpass", "database": "testdb",
			"batch_size": 5,
		}},
		Sink: config.ConnectorConfig{Type: "postgresql", Connection: map[string]any{
			"host": pgEnv.PGHost, "port": pgEnv.PGPort, "user": pgEnv.PGUser,
			"password": pgEnv.PGPassword, "database": pgEnv.PGDatabase, "ssl_mode": "disable", "search_path": "public",
		}},
		Parallelism:   1,
		ErrorHandling: config.ErrorConfig{Mode: "fail_fast", MaxRetries: 2},
		Checkpoint:    config.CheckpointConfig{Enabled: false},
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := p.Run(ctx); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	// Verify all 12 rows were migrated.
	pgPool, err := pgxpool.New(ctx, pgEnv.PGDSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	var count int
	if err := pgPool.QueryRow(ctx, `SELECT COUNT(*) FROM "pagination_test"`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 12 {
		t.Errorf("row count = %d, want 12 (pagination verification failed)", count)
	}

	// Verify a specific value from the last batch.
	var val string
	if err := pgPool.QueryRow(ctx, `SELECT "val" FROM "pagination_test" WHERE "id" = '10'`).Scan(&val); err != nil {
		t.Fatalf("query last batch: %v", err)
	}
	if val != "value-9" {
		t.Errorf("val = %q, want value-9", val)
	}

	fmt.Println("clickhouse pagination test passed")
}
