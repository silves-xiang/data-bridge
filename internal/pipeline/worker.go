package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/silves-xiang/data-bridge/pkg/hook"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// migrateTable migrates a single table from source to sink.
func (p *Pipeline) migrateTable(ctx context.Context, table source.TableInfo) error {
	startTime := time.Now()

	// Check if already complete.
	if p.ckptIsComplete(table.Name) {
		slog.Info("table already completed, skipping", "table", table.Name)
		return nil
	}

	// Fire OnTableStart hooks.
	meta := hook.TableMeta{
		Schema:        table.Schema,
		Name:          table.Name,
		Columns:       table.Columns,
		PrimaryKey:    table.PrimaryKey,
		EstimatedRows: table.EstimatedRows,
	}
	for _, h := range p.tableHooks {
		if err := h.OnTableStart(ctx, meta); err != nil {
			slog.Warn("OnTableStart hook failed", "hook", h.Name(), "error", err)
		}
	}

	var (
		offset       = p.ckptGetOffset(table.Name)
		totalRows    uint64
		batchesCount uint64
		tableErr     error
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchStart := time.Now()
		batch, err := p.source.ReadBatch(ctx, table, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			tableErr = fmt.Errorf("read batch at offset %d: %w", offset, err)
			if p.errorMode == "skip_table" {
				slog.Error("skipping table after read error", "table", table.Name, "error", tableErr)
				break
			}
			return tableErr
		}

		if len(batch.Rows) == 0 {
			break
		}

		// Write batch.
		written, err := p.retryWriteBatch(ctx, table, batch.Rows)
		if err != nil {
			tableErr = fmt.Errorf("write batch at offset %d: %w", offset, err)
			if p.errorMode == "skip_table" {
				slog.Error("skipping table after write error", "table", table.Name, "error", tableErr)
				break
			}
			return tableErr
		}

		totalRows += uint64(written)
		batchesCount++
		batchDuration := time.Since(batchStart)

		// Update checkpoint.
		p.ckptSetOffset(table.Name, offset+1)
		p.ckptAddRows(table.Name, uint64(written))
		p.ckptSave()

		// Debug logging.
		if p.debugEnabled && p.verboseBatch {
			rate := float64(written) / batchDuration.Seconds()
			slog.Debug("batch complete",
				"table", table.Name,
				"offset", offset,
				"rows", written,
				"duration", batchDuration,
				"rate_rows_per_sec", fmt.Sprintf("%.0f", rate),
				"total_rows", totalRows,
			)
		}

		// Fire OnBatchComplete hooks.
		batchResult := hook.BatchResult{
			TableName:   table.Name,
			Offset:      offset,
			RowsInBatch: written,
			RowsSoFar:   totalRows,
			Duration:    batchDuration,
		}
		for _, h := range p.batchHooks {
			if err := h.OnBatchComplete(ctx, batchResult, p.sink.Executor()); err != nil {
				slog.Warn("OnBatchComplete hook failed", "hook", h.Name(), "error", err)
			}
		}

		if batch.IsLast {
			break
		}
		offset++
	}

	// Mark table complete.
	p.ckptMarkComplete(table.Name)
	p.ckptSave()

	// Fire OnTableEnd hooks.
	result := hook.TableResult{
		Schema:       table.Schema,
		Name:         table.Name,
		RowsCopied:   totalRows,
		BatchesCount: batchesCount,
		Duration:     time.Since(startTime),
		Error:        tableErr,
	}
	for _, h := range p.tableHooks {
		if err := h.OnTableEnd(ctx, result, p.sink.Executor()); err != nil {
			slog.Warn("OnTableEnd hook failed", "hook", h.Name(), "error", err)
		}
	}

	slog.Info("table migration complete",
		"table", table.Name,
		"rows", totalRows,
		"batches", batchesCount,
		"duration", time.Since(startTime),
	)

	return tableErr
}

// retryWriteBatch attempts to write a batch with retries on transient errors.
func (p *Pipeline) retryWriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	var lastErr error
	for attempt := 0; attempt < p.maxRetries; attempt++ {
		written, err := p.sink.WriteBatch(ctx, table, rows)
		if err == nil {
			return written, nil
		}
		lastErr = err
		if !isRetryable(err) {
			return 0, err
		}
		delay := time.Duration(1<<attempt) * time.Second
		slog.Warn("retrying write batch", "table", table.Name, "attempt", attempt+1, "delay", delay)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}
	}
	return 0, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// isRetryable determines if an error is transient and worth retrying.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	retryable := []string{
		"connection refused", "connection reset", "broken pipe",
		"deadlock", "serialization", "timeout",
		"too many connections", "connection pool",
	}
	for _, pattern := range retryable {
		if len(msg) >= len(pattern) {
			for i := 0; i <= len(msg)-len(pattern); i++ {
				match := true
				for j := 0; j < len(pattern); j++ {
					c1, c2 := msg[i+j], pattern[j]
					if c1 >= 'A' && c1 <= 'Z' {
						c1 += 32
					}
					if c2 >= 'A' && c2 <= 'Z' {
						c2 += 32
					}
					if c1 != c2 {
						match = false
						break
					}
				}
				if match {
					return true
				}
			}
		}
	}
	return false
}
