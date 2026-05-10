// Package hook defines interfaces for pipeline lifecycle callbacks.
// Hooks allow custom logic to be injected at various stages of migration.
package hook

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// ErrUnknownHook is returned when a hook type is not registered.
var ErrUnknownHook = errors.New("unknown hook type")

// Hook is the base interface all hooks must implement.
type Hook interface {
	// Name returns a unique identifier for this hook instance.
	Name() string

	// Init initializes the hook with user-provided parameters from config.
	Init(params map[string]any) error
}

// PipelineMeta holds metadata passed to OnPipelineStart.
type PipelineMeta struct {
	TaskName    string
	SourceType  string
	SinkType    string
	TablesCount int
	StartedAt   time.Time
}

// PipelineResult holds results passed to OnPipelineEnd.
type PipelineResult struct {
	TaskName     string
	TablesTotal  int
	TablesDone   int
	RowsTotal    uint64
	RowsFailed   uint64
	Duration     time.Duration
	Errors       []string
}

// PipelineHook is implemented by hooks that fire at pipeline start/end.
type PipelineHook interface {
	Hook
	OnPipelineStart(ctx context.Context, meta PipelineMeta) error
	OnPipelineEnd(ctx context.Context, result PipelineResult) error
}

// TableMeta holds metadata passed to OnTableStart.
type TableMeta struct {
	Schema      string
	Name        string
	Columns     []source.ColumnInfo
	PrimaryKey  []string
	EstimatedRows int64
}

// TableResult holds results passed to OnTableEnd.
type TableResult struct {
	Schema       string
	Name         string
	RowsCopied   uint64
	BatchesCount uint64
	Duration     time.Duration
	Error        error
}

// TableHook is implemented by hooks that fire at table start/end.
// Useful for per-table setup (e.g., TimescaleDB hypertable creation).
type TableHook interface {
	Hook
	OnTableStart(ctx context.Context, meta TableMeta) error
	OnTableEnd(ctx context.Context, result TableResult, exec sink.Executor) error
}

// BatchResult holds results passed to OnBatchComplete.
type BatchResult struct {
	TableName   string
	Offset      uint64
	RowsInBatch int
	RowsSoFar   uint64 // Total rows copied for this table so far
	Duration    time.Duration
}

// BatchHook is implemented by hooks that fire after each batch.
// Useful for periodic aggregation or progress-based actions.
type BatchHook interface {
	Hook
	OnBatchComplete(ctx context.Context, result BatchResult, exec sink.Executor) error
}

// Factory creates a new Hook instance.
type Factory func() Hook

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register registers a hook factory. Call from init() in hook plugin packages.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("hook %q already registered", name))
	}
	registry[name] = f
}

// Get returns the factory for a named hook.
func Get(name string) (Factory, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownHook, name)
	}
	return f, nil
}

// List returns all registered hook names.
func List() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
