// Package timescale provides a hook for TimescaleDB-specific operations.
// It creates hypertables and sets up compression/retention policies after data migration.
package timescale

import (
	"context"
	"fmt"

	"github.com/silves-xiang/data-bridge/pkg/hook"
	"github.com/silves-xiang/data-bridge/pkg/sink"
)

// Hook implements hook.TableHook for TimescaleDB hypertable setup.
type Hook struct {
	name                 string
	hypertableInterval   string // e.g., "7 days"
	partitionColumn      string // required: timestamp column for hypertable
	enableCompression    bool
	compressionAfter     string // e.g., "30 days"
	compressionSegmentBy string
	enableRetention      bool
	retentionAfter       string // e.g., "90 days"
}

// Name returns the hook instance name.
func (h *Hook) Name() string {
	return h.name
}

// Init configures the hook from user parameters.
func (h *Hook) Init(params map[string]any) error {
	if v, ok := params["hypertable_interval"].(string); ok {
		h.hypertableInterval = v
	}
	if v, ok := params["partition_column"].(string); ok {
		h.partitionColumn = v
	}
	if v, ok := params["enable_compression"].(bool); ok {
		h.enableCompression = v
	}
	if v, ok := params["compression_after"].(string); ok {
		h.compressionAfter = v
	}
	if v, ok := params["compression_segment_by"].(string); ok {
		h.compressionSegmentBy = v
	}
	if v, ok := params["enable_retention"].(bool); ok {
		h.enableRetention = v
	}
	if v, ok := params["retention_after"].(string); ok {
		h.retentionAfter = v
	}

	if h.hypertableInterval == "" {
		h.hypertableInterval = "7 days"
	}

	return nil
}

// OnTableStart is called before table migration begins.
func (h *Hook) OnTableStart(ctx context.Context, meta hook.TableMeta) error {
	return nil
}

// OnTableEnd is called after table migration completes.
func (h *Hook) OnTableEnd(ctx context.Context, result hook.TableResult, exec sink.Executor) error {
	if result.Error != nil {
		return nil
	}

	if h.partitionColumn == "" {
		return fmt.Errorf("timescale: partition_column is required for table %q", result.Name)
	}

	tableName := fmt.Sprintf("%q.%q", result.Schema, result.Name)
	if result.Schema == "" {
		tableName = fmt.Sprintf("%q", result.Name)
	}

	// Create hypertable.
	htQuery := fmt.Sprintf(
		"SELECT create_hypertable(%s, %q, chunk_time_interval => INTERVAL '%s', if_not_exists => TRUE)",
		tableName, h.partitionColumn, h.hypertableInterval,
	)
	if err := exec.Exec(ctx, htQuery); err != nil {
		return fmt.Errorf("timescale: create_hypertable on %s: %w", tableName, err)
	}

	// Set up compression if enabled.
	if h.enableCompression && h.compressionAfter != "" {
		segBy := h.compressionSegmentBy
		if segBy == "" {
			segBy = h.partitionColumn
		}
		compQuery := fmt.Sprintf(
			"SELECT add_compression_policy(%s, INTERVAL '%s', if_not_exists => TRUE)",
			tableName, h.compressionAfter,
		)
		if err := exec.Exec(ctx, compQuery); err != nil {
			return fmt.Errorf("timescale: add_compression_policy on %s: %w", tableName, err)
		}
	}

	// Set up retention if enabled.
	if h.enableRetention && h.retentionAfter != "" {
		retQuery := fmt.Sprintf(
			"SELECT add_retention_policy(%s, INTERVAL '%s', if_not_exists => TRUE)",
			tableName, h.retentionAfter,
		)
		if err := exec.Exec(ctx, retQuery); err != nil {
			return fmt.Errorf("timescale: add_retention_policy on %s: %w", tableName, err)
		}
	}

	return nil
}

var _ hook.TableHook = (*Hook)(nil)

func init() {
	hook.Register("timescale", func() hook.Hook {
		return &Hook{
			hypertableInterval: "7 days",
		}
	})
}
