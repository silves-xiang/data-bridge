// Package config handles YAML configuration parsing, validation, and env var substitution.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// envVarPattern matches ${VAR_NAME} patterns.
var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// Config is the top-level configuration structure.
type Config struct {
	Task      TaskConfig      `yaml:"task"`
	Source    ConnectorConfig `yaml:"source"`
	Sink      ConnectorConfig `yaml:"sink"`
	Tables    []TableConfig   `yaml:"tables"`
	Parallelism int           `yaml:"parallelism"`
	ErrorHandling ErrorConfig `yaml:"error_handling"`
	Checkpoint   CheckpointConfig `yaml:"checkpoint"`
	Hooks        []HookConfig     `yaml:"hooks"`
	Debug        DebugConfig      `yaml:"debug"`
	Pprof        PprofConfig      `yaml:"pprof"`
}

// TaskConfig identifies the migration task.
type TaskConfig struct {
	Name string `yaml:"name"`
	Mode string `yaml:"mode"` // "full" (future: "incremental")
}

// ConnectorConfig specifies a source or sink connector.
type ConnectorConfig struct {
	Type       string         `yaml:"type"`
	Connection map[string]any `yaml:"connection"`
	Params     map[string]any `yaml:"params"`
}

// TableConfig specifies migration rules for a single table.
type TableConfig struct {
	Source       string             `yaml:"source"`
	Target       string             `yaml:"target"`
	BatchSize    int                `yaml:"batch_size"`
	Where        string             `yaml:"where"`
	Skip         bool               `yaml:"skip"`
	Columns      []ColumnMapping    `yaml:"columns"`
	TypeMappings []TypeMapping      `yaml:"type_mappings"`
}

// ColumnMapping maps a source column to a target column.
type ColumnMapping struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

// TypeMapping overrides the default type mapping for a column.
type TypeMapping struct {
	SourceType string `yaml:"source_type"`
	TargetType string `yaml:"target_type"`
}

// ErrorConfig controls error handling behavior.
type ErrorConfig struct {
	Mode          string `yaml:"mode"` // fail_fast, skip_row, skip_table
	MaxRetries    int    `yaml:"max_retries_per_table"`
	RetryDelay    string `yaml:"retry_delay"`
}

// CheckpointConfig controls checkpoint/resume behavior.
type CheckpointConfig struct {
	Enabled bool   `yaml:"enabled"`
	Dir     string `yaml:"dir"`
}

// HookConfig specifies a hook to load.
type HookConfig struct {
	Name   string         `yaml:"name"`
	Type   string         `yaml:"type"`
	Params map[string]any `yaml:"params"`
}

// DebugConfig controls debug logging and diagnostics.
type DebugConfig struct {
	Enabled      bool `yaml:"enabled"`
	VerboseBatch bool `yaml:"verbose_batch"`
	LogMemory    bool `yaml:"log_memory"`
}

// PprofConfig controls pprof profile collection.
type PprofConfig struct {
	Enabled     bool     `yaml:"enabled"`
	Dir         string   `yaml:"dir"`
	Interval    string   `yaml:"interval"`
	Profiles    []string `yaml:"profiles"`     // heap, goroutine, allocs, cpu
	CPUDuration string   `yaml:"cpu_duration"` // e.g., "30s"
}

// Load reads and parses a YAML config file, applying env var substitution and defaults.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Substitute environment variables.
	data = []byte(substituteEnv(string(data)))

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Apply defaults.
	cfg.applyDefaults()

	// Validate.
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// substituteEnv replaces ${VAR_NAME} with the corresponding environment variable value.
func substituteEnv(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		// Extract the variable name from ${VAR_NAME}.
		varName := match[2 : len(match)-1]
		if val, ok := os.LookupEnv(varName); ok {
			return val
		}
		return match
	})
}

// applyDefaults sets default values for optional fields.
func (c *Config) applyDefaults() {
	if c.Task.Mode == "" {
		c.Task.Mode = "full"
	}
	if c.Parallelism <= 0 {
		c.Parallelism = 4
	}
	if c.ErrorHandling.Mode == "" {
		c.ErrorHandling.Mode = "fail_fast"
	}
	if c.ErrorHandling.MaxRetries == 0 {
		c.ErrorHandling.MaxRetries = 3
	}
	if c.ErrorHandling.RetryDelay == "" {
		c.ErrorHandling.RetryDelay = "5s"
	}
	if c.Checkpoint.Dir == "" {
		c.Checkpoint.Dir = "./.databridge/checkpoints"
	}
	if c.Pprof.Dir == "" {
		c.Pprof.Dir = "./.databridge/pprof"
	}
	if c.Pprof.Interval == "" {
		c.Pprof.Interval = "5m"
	}
	if c.Pprof.CPUDuration == "" {
		c.Pprof.CPUDuration = "30s"
	}
}

// validate checks that required fields are present and values are valid.
func (c *Config) validate() error {
	if c.Task.Name == "" {
		return fmt.Errorf("task.name is required")
	}

	if c.Source.Type == "" {
		return fmt.Errorf("source.type is required")
	}
	if c.Sink.Type == "" {
		return fmt.Errorf("sink.type is required")
	}

	validModes := map[string]bool{"fail_fast": true, "skip_row": true, "skip_table": true}
	if !validModes[c.ErrorHandling.Mode] {
		return fmt.Errorf("error_handling.mode must be one of: fail_fast, skip_row, skip_table, got %q", c.ErrorHandling.Mode)
	}

	validModes2 := map[string]bool{"full": true, "incremental": true}
	if !validModes2[c.Task.Mode] {
		return fmt.Errorf("task.mode must be one of: full, incremental, got %q", c.Task.Mode)
	}

	// Validate env vars are resolved (warn about unresolved placeholders).
	for _, line := range strings.Split(strings.TrimSpace(fmt.Sprintf("%v", c.Source.Connection)), "\n") {
		if envVarPattern.MatchString(line) {
			// Unresolved env var — could warn here.
			// For now, we don't fail because some env vars may be set at runtime.
		}
	}

	return nil
}

// HasTableConfig returns whether specific table configurations are defined.
func (c *Config) HasTableConfig() bool {
	return len(c.Tables) > 0
}

// GetTableConfig returns the config for a specific table, if defined.
func (c *Config) GetTableConfig(sourceName string) *TableConfig {
	for i := range c.Tables {
		if c.Tables[i].Source == sourceName {
			return &c.Tables[i]
		}
	}
	return nil
}
