package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	// Create a temporary config file.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")

	content := `
task:
  name: "test-migration"
source:
  type: mysql
  connection:
    host: "localhost"
    port: 3306
    user: "root"
    password: "${TEST_PASSWORD}"
    database: "test_db"
sink:
  type: postgresql
  connection:
    host: "localhost"
    port: 5432
    user: "postgres"
    password: "${PG_PASSWORD}"
    database: "test_db"
tables:
  - source: "users"
    target: "users_new"
parallelism: 4
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	os.Setenv("TEST_PASSWORD", "secret123")
	defer os.Unsetenv("TEST_PASSWORD")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Task.Name != "test-migration" {
		t.Errorf("task name = %q, want %q", cfg.Task.Name, "test-migration")
	}
	if cfg.Task.Mode != "full" {
		t.Errorf("task mode = %q, want %q", cfg.Task.Mode, "full")
	}
	if cfg.Source.Type != "mysql" {
		t.Errorf("source type = %q, want %q", cfg.Source.Type, "mysql")
	}
	if cfg.Sink.Type != "postgresql" {
		t.Errorf("sink type = %q, want %q", cfg.Sink.Type, "postgresql")
	}
	if cfg.Parallelism != 4 {
		t.Errorf("parallelism = %d, want 4", cfg.Parallelism)
	}

	// Verify env var substitution.
	pw, ok := cfg.Source.Connection["password"].(string)
	if !ok || pw != "secret123" {
		t.Errorf("password = %q, want %q (env var not substituted)", pw, "secret123")
	}

	// Verify table config.
	if len(cfg.Tables) != 1 {
		t.Fatalf("tables count = %d, want 1", len(cfg.Tables))
	}
	if cfg.Tables[0].Source != "users" {
		t.Errorf("table source = %q, want %q", cfg.Tables[0].Source, "users")
	}
	if cfg.Tables[0].Target != "users_new" {
		t.Errorf("table target = %q, want %q", cfg.Tables[0].Target, "users_new")
	}
}

func TestLoadDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.yaml")

	content := `
task:
  name: "minimal"
source:
  type: mysql
  connection:
    host: "localhost"
sink:
  type: postgresql
  connection:
    host: "localhost"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Parallelism != 4 {
		t.Errorf("default parallelism = %d, want 4", cfg.Parallelism)
	}
	if cfg.ErrorHandling.Mode != "fail_fast" {
		t.Errorf("default error mode = %q, want fail_fast", cfg.ErrorHandling.Mode)
	}
	if cfg.Checkpoint.Dir != "./.databridge/checkpoints" {
		t.Errorf("default checkpoint dir = %q", cfg.Checkpoint.Dir)
	}
}

func TestLoadValidationError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.yaml")

	content := `
task:
  name: ""
source:
  type: mysql
  connection: {}
sink:
  type: postgresql
  connection: {}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}
