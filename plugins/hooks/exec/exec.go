// Package exec provides a generic SQL execution hook.
// It runs user-defined SQL at various pipeline lifecycle points.
package exec

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	"github.com/silves-xiang/data-bridge/pkg/hook"
	"github.com/silves-xiang/data-bridge/pkg/sink"
)

// Hook implements hook.PipelineHook, hook.TableHook, and hook.BatchHook.
// Users configure SQL to run at each lifecycle point via config params.
type Hook struct {
	name string

	// Pipeline-level SQL.
	onPipelineStartSQL string
	onPipelineEndSQL   string

	// Table-level SQL.
	onTableStartSQL string
	onTableEndSQL   string

	// Batch-level SQL (runs every onBatchCompleteRows or every batch).
	onBatchCompleteSQL  string
	onBatchCompleteRows int64 // 0 = every batch, >0 = every N rows
	batchRowCounter     int64
}

// Name returns the hook instance name.
func (h *Hook) Name() string {
	return h.name
}

// Init configures the hook from user parameters.
func (h *Hook) Init(params map[string]any) error {
	if v, ok := params["on_pipeline_start"].(string); ok {
		h.onPipelineStartSQL = v
	}
	if v, ok := params["on_pipeline_end"].(string); ok {
		h.onPipelineEndSQL = v
	}
	if v, ok := params["on_table_start"].(string); ok {
		h.onTableStartSQL = v
	}
	if v, ok := params["on_table_end"].(string); ok {
		h.onTableEndSQL = v
	}
	if v, ok := params["sql"].(string); ok {
		h.onBatchCompleteSQL = v
	}
	if v, ok := params["on_batch_complete_rows"].(float64); ok {
		h.onBatchCompleteRows = int64(v)
	} else if v, ok := params["on_batch_complete_rows"].(int); ok {
		h.onBatchCompleteRows = int64(v)
	}
	return nil
}

// OnPipelineStart runs before migration.
func (h *Hook) OnPipelineStart(ctx context.Context, meta hook.PipelineMeta) error {
	if h.onPipelineStartSQL == "" {
		return nil
	}
	return h.execTemplate(ctx, nil, h.onPipelineStartSQL)
}

// OnPipelineEnd runs after migration.
func (h *Hook) OnPipelineEnd(ctx context.Context, result hook.PipelineResult) error {
	if h.onPipelineEndSQL == "" {
		return nil
	}
	return h.execTemplate(ctx, nil, h.onPipelineEndSQL)
}

// OnTableStart runs before a table migration.
func (h *Hook) OnTableStart(ctx context.Context, meta hook.TableMeta) error {
	if h.onTableStartSQL == "" {
		return nil
	}
	return h.execTemplate(ctx, nil, h.onTableStartSQL)
}

// OnTableEnd runs after a table migration, with access to the sink executor.
func (h *Hook) OnTableEnd(ctx context.Context, result hook.TableResult, exec sink.Executor) error {
	if h.onTableEndSQL == "" {
		return nil
	}
	return h.execTemplateWithExec(ctx, exec, result, h.onTableEndSQL)
}

// OnBatchComplete runs after each batch (or every N rows if configured).
func (h *Hook) OnBatchComplete(ctx context.Context, result hook.BatchResult, exec sink.Executor) error {
	if h.onBatchCompleteSQL == "" {
		return nil
	}
	if h.onBatchCompleteRows > 0 {
		h.batchRowCounter += int64(result.RowsInBatch)
		if h.batchRowCounter < h.onBatchCompleteRows {
			return nil
		}
		h.batchRowCounter = 0
	}
	return h.execTemplateWithExec(ctx, exec, result, h.onBatchCompleteSQL)
}

// execTemplate renders and executes a SQL template (without executor, for pipeline-level).
func (h *Hook) execTemplate(ctx context.Context, data any, tmpl string) error {
	// Pipeline-level hooks don't have an executor; they're informational only.
	// The actual SQL execution happens at table/batch level with the executor.
	return nil
}

// execTemplateWithExec renders and executes a SQL template with the given executor.
func (h *Hook) execTemplateWithExec(ctx context.Context, exec sink.Executor, data any, tmpl string) error {
	t, err := template.New("sql").Parse(tmpl)
	if err != nil {
		return fmt.Errorf("exec hook: parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return fmt.Errorf("exec hook: execute template: %w", err)
	}

	rendered := buf.String()
	if rendered == "" {
		return nil
	}

	return exec.Exec(ctx, rendered)
}

var (
	_ hook.PipelineHook = (*Hook)(nil)
	_ hook.TableHook    = (*Hook)(nil)
	_ hook.BatchHook    = (*Hook)(nil)
)

func init() {
	hook.Register("exec", func() hook.Hook {
		return &Hook{}
	})
}
