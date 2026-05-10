// Package sink defines the interface for writing data to a target database.
package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/silves-xiang/data-bridge/pkg/source"
)

// ErrUnknownSink is returned when a sink type is not registered.
var ErrUnknownSink = errors.New("unknown sink type")

// Executor allows hooks to run SQL against the target database.
type Executor interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (Rows, error)
}

// Rows is a minimal row iterator interface to avoid exposing database/sql directly.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

// Sink writes data to a target database.
type Sink interface {
	// Executor returns a query executor for the sink connection. Used by hooks
	// to run custom SQL (e.g. CREATE INDEX, ANALYZE, hypertable setup).
	Executor() Executor

	// Open establishes the connection.
	Open(ctx context.Context, config map[string]any) error

	// Close tears down the connection.
	Close() error

	// CreateTable creates the target table using the mapped ColumnInfo.
	CreateTable(ctx context.Context, table source.TableInfo) error

	// WriteBatch inserts a batch of rows. Returns the number of rows written.
	WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error)

	// PrepareTarget runs before migration (disable constraints, drop indexes).
	PrepareTarget(ctx context.Context, tables []source.TableInfo) error

	// CleanupTarget runs after migration (re-enable constraints, ANALYZE).
	CleanupTarget(ctx context.Context) error
}

// Factory creates a new Sink instance.
type Factory func() Sink

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register registers a sink factory. Call from init() in plugin packages.
// If a sink with the same name is already registered, it is overwritten with a warning.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		slog.Warn("sink already registered, overwriting", "name", name)
	}
	registry[name] = f
}

// Unregister removes a sink factory. Safe to call on non-existent names.
func Unregister(name string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, name)
}

// Get returns the factory for a named sink.
func Get(name string) (Factory, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSink, name)
	}
	return f, nil
}

// List returns all registered sink names.
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
