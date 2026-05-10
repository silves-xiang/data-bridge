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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"

	_ "github.com/silves-xiang/data-bridge/plugins/mongodb"
	_ "github.com/silves-xiang/data-bridge/plugins/postgresql"
)

func setupMongoDB(t *testing.T) (uri string, teardown func()) {
	t.Helper()
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:7",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections").WithStartupTimeout(2 * time.Minute),
	}

	ctn, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start mongodb: %v", err)
	}

	host, _ := ctn.Host(ctx)
	port, _ := ctn.MappedPort(ctx, "27017/tcp")
	portNum, _ := strconv.Atoi(port.Port())

	uri = fmt.Sprintf("mongodb://%s:%d", host, portNum)
	return uri, func() { ctn.Terminate(ctx) }
}

func TestMongoDBToPostgres(t *testing.T) {
	pgEnv := SetupTestEnv(t)
	ctx := context.Background()
	defer pgEnv.Teardown(ctx)

	mongoURI, mongoTeardown := setupMongoDB(t)
	defer mongoTeardown()

	t.Logf("MongoDB: %s", mongoURI)

	// Seed data in MongoDB directly.
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI).
		SetConnectTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("connect mongodb: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	coll := mongoClient.Database("testdb").Collection("users")
	docs := []any{
		bson.M{"name": "Alice", "email": "alice@example.com", "age": int32(30), "active": true},
		bson.M{"name": "Bob", "email": "bob@test.org", "age": int32(25), "active": false},
		bson.M{"name": "Charlie", "email": "charlie@demo.com", "age": int32(35)},
		bson.M{"name": "Diana"},
		bson.M{"name": "Eve", "email": "eve@example.com", "age": int32(28), "active": true, "score": 95.5},
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Log("seeded 5 documents into MongoDB")

	// Migrate MongoDB → PostgreSQL.
	cfg := &config.Config{
		Task: config.TaskConfig{Name: "test-mongo-to-pg", Mode: "full"},
		Source: config.ConnectorConfig{Type: "mongodb", Connection: map[string]any{
			"uri": mongoURI, "database": "testdb",
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

	fmt.Println("mongodb → postgres test passed")
}
