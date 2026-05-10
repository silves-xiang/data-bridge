//go:build integration
// +build integration

// Package integration provides end-to-end tests using testcontainers.
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	// Register connectors.
	_ "github.com/silves-xiang/data-bridge/plugins/mysql"
	_ "github.com/silves-xiang/data-bridge/plugins/postgresql"
)

// TestEnv holds connection details for running test containers.
type TestEnv struct {
	MySQLHost     string
	MySQLPort     int
	MySQLUser     string
	MySQLPassword string
	MySQLDatabase string

	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string

	mysqlCtn    *mysql.MySQLContainer
	postgresCtn *postgres.PostgresContainer
}

// SetupTestEnv starts MySQL and PostgreSQL containers and initializes schemas.
func SetupTestEnv(t *testing.T) *TestEnv {
	t.Helper()

	// Skip ryuk reaper container to avoid pulling testcontainers/ryuk from Docker Hub.
	// Containers will still be cleaned up by Teardown.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()

	env := &TestEnv{
		MySQLUser:     "root",
		MySQLPassword: "testpass",
		MySQLDatabase: "source_db",
		PGUser:        "postgres",
		PGPassword:    "testpass",
		PGDatabase:    "target_db",
	}

	// Start MySQL.
	mysqlCtn, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase(env.MySQLDatabase),
		mysql.WithUsername(env.MySQLUser),
		mysql.WithPassword(env.MySQLPassword),
	)
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	env.mysqlCtn = mysqlCtn

	mysqlHost, err := mysqlCtn.Host(ctx)
	if err != nil {
		t.Fatalf("mysql host: %v", err)
	}
	mysqlPort, err := mysqlCtn.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("mysql port: %v", err)
	}
	env.MySQLHost = mysqlHost
	env.MySQLPort = int(mysqlPort.Num())

	// Start PostgreSQL.
	postgresCtn, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(env.PGDatabase),
		postgres.WithUsername(env.PGUser),
		postgres.WithPassword(env.PGPassword),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	env.postgresCtn = postgresCtn

	pgHost, err := postgresCtn.Host(ctx)
	if err != nil {
		t.Fatalf("postgres host: %v", err)
	}
	pgPort, err := postgresCtn.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("postgres port: %v", err)
	}
	env.PGHost = pgHost
	env.PGPort = int(pgPort.Num())

	t.Logf("MySQL: %s:%d", env.MySQLHost, env.MySQLPort)
	t.Logf("PostgreSQL: %s:%d", env.PGHost, env.PGPort)

	// Wait for databases to be ready by pinging them with retries.
	waitForDB(t, ctx, env)

	return env
}

// Teardown stops and removes the containers.
func (env *TestEnv) Teardown(ctx context.Context) {
	if env.postgresCtn != nil {
		env.postgresCtn.Terminate(ctx)
	}
	if env.mysqlCtn != nil {
		env.mysqlCtn.Terminate(ctx)
	}
}

// MySQLDSN returns the MySQL data source name.
func (env *TestEnv) MySQLDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true",
		env.MySQLUser, env.MySQLPassword, env.MySQLHost, env.MySQLPort, env.MySQLDatabase)
}

// PGDSN returns the PostgreSQL connection string.
func (env *TestEnv) PGDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		env.PGHost, env.PGPort, env.PGUser, env.PGPassword, env.PGDatabase)
}

// waitForDB pings MySQL and PostgreSQL with retries until they are ready.
func waitForDB(t *testing.T, ctx context.Context, env *TestEnv) {
	t.Helper()

	// Try connecting to MySQL with retries.
	for i := 0; i < 30; i++ {
		db, err := sql.Open("mysql", env.MySQLDSN())
		if err == nil {
			err = db.PingContext(ctx)
			db.Close()
		}
		if err == nil {
			break
		}
		if i == 29 {
			t.Fatalf("mysql not ready after 30 retries: %v", err)
		}
		time.Sleep(time.Second)
	}

	// Try connecting to PostgreSQL with retries.
	for i := 0; i < 30; i++ {
		db, err := sql.Open("pgx", env.PGDSN())
		if err == nil {
			err = db.PingContext(ctx)
			db.Close()
		}
		if err == nil {
			break
		}
		if i == 29 {
			t.Fatalf("postgres not ready after 30 retries: %v", err)
		}
		time.Sleep(time.Second)
	}
}
