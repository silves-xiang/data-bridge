//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"

	// Register connectors.
	_ "github.com/silves-xiang/data-bridge/plugins/influxdb"
	_ "github.com/silves-xiang/data-bridge/plugins/mysql"
	_ "github.com/silves-xiang/data-bridge/plugins/postgresql"
)

// influxEnv holds InfluxDB test container connection details.
type influxEnv struct {
	URL    string
	Token  string
	Org    string
	Bucket string
	Port   int
	ctn    testcontainers.Container
}

// setupInfluxDB starts an InfluxDB 2.x container and returns connection info.
func setupInfluxDB(t *testing.T) *influxEnv {
	t.Helper()

	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "influxdb:2.7",
		ExposedPorts: []string{"8086/tcp"},
		Env: map[string]string{
			"DOCKER_INFLUXDB_INIT_MODE":        "setup",
			"DOCKER_INFLUXDB_INIT_USERNAME":    "admin",
			"DOCKER_INFLUXDB_INIT_PASSWORD":    "password123",
			"DOCKER_INFLUXDB_INIT_ORG":         "testorg",
			"DOCKER_INFLUXDB_INIT_BUCKET":      "testbucket",
			"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN": "test-token-super-secret",
			"INFLUXD_LOG_LEVEL":                "error",
		},
		WaitingFor: wait.ForHTTP("/health").WithPort("8086/tcp").WithStartupTimeout(2 * time.Minute),
	}

	ctn, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start influxdb: %v", err)
	}

	host, err := ctn.Host(ctx)
	if err != nil {
		t.Fatalf("influxdb host: %v", err)
	}
	port, err := ctn.MappedPort(ctx, "8086/tcp")
	if err != nil {
		t.Fatalf("influxdb port: %v", err)
	}
	portNum, _ := strconv.Atoi(port.Port())

	env := &influxEnv{
		URL:    fmt.Sprintf("http://%s:%d", host, portNum),
		Token:  "test-token-super-secret",
		Org:    "testorg",
		Bucket: "testbucket",
		Port:   portNum,
		ctn:    ctn,
	}

	// Wait for InfluxDB to be ready by pinging.
	client := influxdb2.NewClient(env.URL, env.Token)
	defer client.Close()

	for i := 0; i < 30; i++ {
		ok, pingErr := client.Ping(ctx)
		if ok && pingErr == nil {
			break
		}
		if i == 29 {
			t.Fatalf("influxdb not ready after 30 retries")
		}
		time.Sleep(time.Second)
	}

	t.Logf("InfluxDB: %s (org=%s bucket=%s)", env.URL, env.Org, env.Bucket)
	return env
}

func (env *influxEnv) Teardown(ctx context.Context) {
	if env.ctn != nil {
		env.ctn.Terminate(ctx)
	}
}

// seedInflux writes test data directly to the InfluxDB bucket.
func seedInflux(ctx context.Context, t *testing.T, env *influxEnv) {
	t.Helper()

	client := influxdb2.NewClient(env.URL, env.Token)
	defer client.Close()

	writeAPI := client.WriteAPI(env.Org, env.Bucket)

	// Write temperature readings with tags (use recent timestamps).
	baseTime := time.Now().Add(-10 * time.Minute).UTC()
	for i := 0; i < 5; i++ {
		ptTime := baseTime.Add(time.Duration(i) * time.Minute)
		p := influxdb2.NewPoint(
			"temperature",
			map[string]string{
				"sensor_id": fmt.Sprintf("sensor-%d", i%2),
				"location":  "datacenter-1",
			},
			map[string]any{
				"value":    22.5 + float64(i),
				"humidity": 55.0 + float64(i)*2,
			},
			ptTime,
		)
		writeAPI.WritePoint(p)
	}
	writeAPI.Flush()

	// Check for errors.
	select {
	case err := <-writeAPI.Errors():
		t.Fatalf("influxdb write: %v", err)
	default:
	}

	t.Logf("seeded 5 points into InfluxDB measurement 'temperature'")
}

// TestInfluxDBToPostgres migrates data from InfluxDB (source) to PostgreSQL (sink).
func TestInfluxDBToPostgres(t *testing.T) {
	pgEnv := SetupTestEnv(t)
	ictx := context.Background()
	defer pgEnv.Teardown(ictx)

	infEnv := setupInfluxDB(t)
	defer infEnv.Teardown(ictx)

	// Seed InfluxDB with test data.
	seedInflux(ictx, t, infEnv)

	cfg := &config.Config{
		Task: config.TaskConfig{Name: "test-influx-to-pg", Mode: "full"},
		Source: config.ConnectorConfig{Type: "influxdb", Connection: map[string]any{
			"url":    infEnv.URL,
			"token":  infEnv.Token,
			"org":    infEnv.Org,
			"bucket": infEnv.Bucket,
		}},
		Sink: config.ConnectorConfig{Type: "postgresql", Connection: map[string]any{
			"host":        pgEnv.PGHost,
			"port":        pgEnv.PGPort,
			"user":        pgEnv.PGUser,
			"password":    pgEnv.PGPassword,
			"database":    pgEnv.PGDatabase,
			"ssl_mode":    "disable",
			"search_path": "public",
		}},
		Parallelism:   1,
		ErrorHandling: config.ErrorConfig{Mode: "fail_fast", MaxRetries: 2},
		Checkpoint:    config.CheckpointConfig{Enabled: false},
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	if err := p.Run(ictx); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	t.Log("migration done")

	// Verify in PostgreSQL.
	pgPool, err := pgxpool.New(ictx, pgEnv.PGDSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	var count int
	if err := pgPool.QueryRow(ictx, `SELECT COUNT(*) FROM "temperature"`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("row count = %d, want 5", count)
	}

	// Spot-check a value.
	var humidity string
	query := `SELECT "humidity" FROM "temperature" WHERE "sensor_id" = 'sensor-0' AND "location" = 'datacenter-1' ORDER BY "_time" LIMIT 1`
	if err := pgPool.QueryRow(ictx, query).Scan(&humidity); err != nil {
		t.Fatalf("query humidity: %v", err)
	}
	if humidity != "55" {
		t.Errorf("humidity = %q, want %q", humidity, "55")
	}

	fmt.Println("influxdb → postgres test passed")
}
