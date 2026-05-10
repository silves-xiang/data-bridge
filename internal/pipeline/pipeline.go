package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/debug"
	"github.com/silves-xiang/data-bridge/pkg/hook"
	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// Pipeline orchestrates the full migration from source to sink.
type Pipeline struct {
	cfg    *config.Config
	source source.Source
	sink   sink.Sink

	// Hooks categorized by interface type.
	pipelineHooks []hook.PipelineHook
	tableHooks    []hook.TableHook
	batchHooks    []hook.BatchHook

	// Checkpoint for resume support.
	checkpoint *Checkpoint

	// Configuration derived from config.
	errorMode    string
	maxRetries   int
	debugEnabled bool
	verboseBatch bool
}

// New creates a new Pipeline from a configuration.
func New(cfg *config.Config) (*Pipeline, error) {
	// Build source.
	srcFactory, err := source.Get(cfg.Source.Type)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	src := srcFactory()

	// Build sink.
	snkFactory, err := sink.Get(cfg.Sink.Type)
	if err != nil {
		return nil, fmt.Errorf("sink: %w", err)
	}
	snk := snkFactory()

	// Build hooks.
	var (
		pipelineHooks []hook.PipelineHook
		tableHooks    []hook.TableHook
		batchHooks    []hook.BatchHook
	)

	for _, hc := range cfg.Hooks {
		hf, err := hook.Get(hc.Type)
		if err != nil {
			return nil, fmt.Errorf("hook %q: %w", hc.Type, err)
		}
		h := hf()
		h.Init(hc.Params)

		if ph, ok := h.(hook.PipelineHook); ok {
			pipelineHooks = append(pipelineHooks, ph)
		}
		if th, ok := h.(hook.TableHook); ok {
			tableHooks = append(tableHooks, th)
		}
		if bh, ok := h.(hook.BatchHook); ok {
			batchHooks = append(batchHooks, bh)
		}
	}

	// Setup checkpoint if enabled.
	var ckpt *Checkpoint
	if cfg.Checkpoint.Enabled {
		ckpt = NewCheckpoint(cfg.Task.Name, cfg.Checkpoint.Dir)
		if err := ckpt.Load(); err != nil {
			slog.Warn("failed to load checkpoint, starting fresh", "error", err)
		}
	}

	return &Pipeline{
		cfg:           cfg,
		source:        src,
		sink:          snk,
		pipelineHooks: pipelineHooks,
		tableHooks:    tableHooks,
		batchHooks:    batchHooks,
		checkpoint:    ckpt,
		errorMode:     cfg.ErrorHandling.Mode,
		maxRetries:    cfg.ErrorHandling.MaxRetries,
		debugEnabled:  cfg.Debug.Enabled,
		verboseBatch:  cfg.Debug.VerboseBatch,
	}, nil
}

// Run executes the full migration pipeline.
func (p *Pipeline) Run(ctx context.Context) error {
	startTime := time.Now()

	// Configure debug logging.
	if p.debugEnabled {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("debug mode enabled")
	}

	// Start pprof collection if configured.
	pprofStop := startPprofIfEnabled(p.cfg)
	if pprofStop != nil {
		defer pprofStop()
	}

	// Configure debug logging.
	debug.SetupDebug(p.cfg.Debug.Enabled, p.cfg.Debug.VerboseBatch, p.cfg.Debug.LogMemory)

	// 1. Open connections.
	srcCfg := mergeConfig(p.cfg.Source.Connection, p.cfg.Source.Params)
	slog.Info("connecting to source", "type", p.cfg.Source.Type)
	if err := p.source.Open(ctx, srcCfg); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer p.source.Close()

	snkCfg := mergeConfig(p.cfg.Sink.Connection, p.cfg.Sink.Params)
	slog.Info("connecting to sink", "type", p.cfg.Sink.Type)
	if err := p.sink.Open(ctx, snkCfg); err != nil {
		return fmt.Errorf("open sink: %w", err)
	}
	defer p.sink.Close()

	// 2. Discover tables.
	tables, err := p.source.Tables(ctx)
	if err != nil {
		return fmt.Errorf("discover tables: %w", err)
	}

	// Filter tables based on config.
	tables = p.filterTables(tables)

	// Normalize schemas: use sink's search_path as the target schema.
	sinkSchema := "public"
	if sp, ok := p.cfg.Sink.Connection["search_path"].(string); ok && sp != "" {
		sinkSchema = sp
	}
	for i := range tables {
		tables[i].Schema = sinkSchema
	}

	if len(tables) == 0 {
		slog.Warn("no tables to migrate")
		return nil
	}

	slog.Info("tables to migrate", "count", len(tables))

	// 3. OnPipelineStart hooks.
	meta := hook.PipelineMeta{
		TaskName:    p.cfg.Task.Name,
		SourceType:  p.cfg.Source.Type,
		SinkType:    p.cfg.Sink.Type,
		TablesCount: len(tables),
		StartedAt:   startTime,
	}
	for _, h := range p.pipelineHooks {
		if err := h.OnPipelineStart(ctx, meta); err != nil {
			slog.Warn("OnPipelineStart hook failed", "hook", h.Name(), "error", err)
		}
	}

	// 4. Prepare target.
	if err := p.sink.PrepareTarget(ctx, tables); err != nil {
		slog.Warn("PrepareTarget failed", "error", err)
	}

	// 5. Create target tables.
	for _, t := range tables {
		if p.ckptIsComplete(t.Name) {
			continue
		}
		if err := p.sink.CreateTable(ctx, t); err != nil {
			slog.Warn("CreateTable failed", "table", t.Name, "error", err)
		}
	}

	// 6. Migrate tables using worker pool.
	sem := make(chan struct{}, p.cfg.Parallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var (
		tablesDone   int
		tablesFailed int
		errors       []string
	)

	for i := range tables {
		table := tables[i]

		// Check if already complete from checkpoint.
		if p.ckptIsComplete(table.Name) {
			mu.Lock()
			tablesDone++
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(t source.TableInfo) {
			defer wg.Done()

			// Acquire semaphore.
			sem <- struct{}{}
			defer func() { <-sem }()

			// Migrate table.
			err := p.migrateTable(ctx, t)

			mu.Lock()
			if err != nil && p.errorMode != "skip_row" {
				tablesFailed++
				errMsg := fmt.Sprintf("table %s: %v", t.Name, err)
				errors = append(errors, errMsg)
				slog.Error("table migration failed", "table", t.Name, "error", err)
			} else {
				tablesDone++
			}
			mu.Unlock()
		}(table)
	}

	wg.Wait()

	// 7. Cleanup target.
	if err := p.sink.CleanupTarget(ctx); err != nil {
		slog.Warn("CleanupTarget failed", "error", err)
	}

	// 8. OnPipelineEnd hooks.
	duration := time.Since(startTime)
	totalRows := p.ckptTotalRows()

	result := hook.PipelineResult{
		TaskName:    p.cfg.Task.Name,
		TablesTotal: len(tables),
		TablesDone:  tablesDone,
		RowsTotal:   totalRows,
		Duration:    duration,
		Errors:      errors,
	}
	for _, h := range p.pipelineHooks {
		if err := h.OnPipelineEnd(ctx, result); err != nil {
			slog.Warn("OnPipelineEnd hook failed", "hook", h.Name(), "error", err)
		}
	}

	// 9. Print summary.
	p.printSummary(result, tablesFailed)

	if tablesFailed > 0 && p.errorMode == "fail_fast" {
		return fmt.Errorf("migration failed: %d table(s) had errors", tablesFailed)
	}

	return nil
}

// filterTables applies table-level configuration filters.
func (p *Pipeline) filterTables(tables []source.TableInfo) []source.TableInfo {
	if !p.cfg.HasTableConfig() {
		return tables
	}

	configMap := make(map[string]*config.TableConfig)
	for i := range p.cfg.Tables {
		configMap[p.cfg.Tables[i].Source] = &p.cfg.Tables[i]
	}

	var filtered []source.TableInfo
	for _, t := range tables {
		tc, ok := configMap[t.Name]
		if ok && tc.Skip {
			continue
		}
		// Apply target name mapping if configured.
		if ok && tc.Target != "" {
			t.Name = tc.Target
		}
		// Apply WHERE clause if configured.
		// (This is informational; actual filtering happens in ReadBatch implementation)
		if ok && tc.BatchSize > 0 {
			// Will be used in migrateTable.
		}
		filtered = append(filtered, t)
	}
	return filtered
}

// tableConfig returns the config for a specific table, if any.
func (p *Pipeline) tableConfig(tableName string) *config.TableConfig {
	return p.cfg.GetTableConfig(tableName)
}

// ---- nil-safe checkpoint wrappers ----

func (p *Pipeline) ckptIsComplete(tableName string) bool {
	if p.checkpoint == nil {
		return false
	}
	return p.checkpoint.IsComplete(tableName)
}

func (p *Pipeline) ckptGetOffset(tableName string) uint64 {
	if p.checkpoint == nil {
		return 0
	}
	return p.checkpoint.GetOffset(tableName)
}

func (p *Pipeline) ckptSave() {
	if p.checkpoint != nil {
		p.checkpoint.Save()
	}
}

func (p *Pipeline) ckptSetOffset(tableName string, offset uint64) {
	if p.checkpoint != nil {
		p.checkpoint.SetOffset(tableName, offset)
	}
}

func (p *Pipeline) ckptAddRows(tableName string, count uint64) {
	if p.checkpoint != nil {
		p.checkpoint.AddRows(tableName, count)
	}
}

func (p *Pipeline) ckptMarkComplete(tableName string) {
	if p.checkpoint != nil {
		p.checkpoint.MarkComplete(tableName)
	}
}

func (p *Pipeline) ckptTotalRows() uint64 {
	if p.checkpoint == nil {
		return 0
	}
	var total uint64
	for _, tc := range p.checkpoint.Tables {
		total += tc.RowsCopied
	}
	return total
}

// startPprofIfEnabled starts pprof collection based on config.
func startPprofIfEnabled(cfg *config.Config) func() {
	pprofCfg := cfg.Pprof
	if !pprofCfg.Enabled {
		return nil
	}

	interval, err := time.ParseDuration(pprofCfg.Interval)
	if err != nil {
		interval = 5 * time.Minute
	}

	cpuDuration, err := time.ParseDuration(pprofCfg.CPUDuration)
	if err != nil {
		cpuDuration = 30 * time.Second
	}

	stop, err := debug.StartPprof(debug.PprofConfig{
		Enabled:     pprofCfg.Enabled,
		Dir:         pprofCfg.Dir,
		Interval:    interval,
		Profiles:    pprofCfg.Profiles,
		CPUDuration: cpuDuration,
	})
	if err != nil {
		slog.Warn("pprof start failed", "error", err)
		return nil
	}
	return stop
}

// printSummary prints a migration summary report.
func (p *Pipeline) printSummary(result hook.PipelineResult, failed int) {
	slog.Info("=== Migration Summary ===",
		"task", result.TaskName,
		"tables_total", result.TablesTotal,
		"tables_done", result.TablesDone,
		"tables_failed", failed,
		"rows_total", result.RowsTotal,
		"duration", result.Duration,
	)
	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			slog.Error("error", "detail", e)
		}
	}
}

// mergeConfig merges params into the connection map, with params taking precedence.
func mergeConfig(conn, params map[string]any) map[string]any {
	if len(params) == 0 {
		return conn
	}
	merged := make(map[string]any, len(conn)+len(params))
	for k, v := range conn {
		merged[k] = v
	}
	for k, v := range params {
		merged[k] = v
	}
	return merged
}
