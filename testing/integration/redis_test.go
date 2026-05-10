//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	redis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"

	_ "github.com/silves-xiang/data-bridge/plugins/postgresql"
	_ "github.com/silves-xiang/data-bridge/plugins/redis"
)

func setupRedis(t *testing.T) (addr string, teardown func()) {
	t.Helper()
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(2 * time.Minute),
	}

	ctn, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}

	host, _ := ctn.Host(ctx)
	port, _ := ctn.MappedPort(ctx, "6379/tcp")
	portNum, _ := strconv.Atoi(port.Port())

	addr = fmt.Sprintf("%s:%d", host, portNum)
	return addr, func() { ctn.Terminate(ctx) }
}

func TestRedisToPostgres(t *testing.T) {
	pgEnv := SetupTestEnv(t)
	ctx := context.Background()
	defer pgEnv.Teardown(ctx)

	redisAddr, redisTeardown := setupRedis(t)
	defer redisTeardown()

	t.Logf("Redis: %s", redisAddr)

	// Seed data in Redis directly.
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// Create hash keys with user: prefix.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("user:%d", i+1)
		rdb.HSet(ctx, key,
			"name", fmt.Sprintf("User-%d", i),
			"email", fmt.Sprintf("user%d@example.com", i),
			"age", fmt.Sprintf("%d", 20+i),
		)
	}
	t.Log("seeded 5 hash keys into Redis")

	// Migrate Redis → PostgreSQL.
	cfg := &config.Config{
		Task: config.TaskConfig{Name: "test-redis-to-pg", Mode: "full"},
		Source: config.ConnectorConfig{Type: "redis", Connection: map[string]any{
			"addr": redisAddr,
			"db":   0,
			"key_patterns": map[string]any{
				"users": "user:*",
			},
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
	if err := pgPool.QueryRow(ctx, `SELECT COUNT(*) FROM "users"`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("row count = %d, want 5", count)
	}

	fmt.Println("redis → postgres test passed")
}
