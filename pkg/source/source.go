// Package source defines the interface for reading data from a source database.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"sort"
	"sync"
)

// ErrUnknownSource is returned when a source type is not registered.
var ErrUnknownSource = errors.New("unknown source type")

// Source reads data from a database.
type Source interface {
	// Open establishes the connection. The config map contains connection parameters
	// parsed from the YAML config (host, port, user, password, database, etc.).
	Open(ctx context.Context, config map[string]any) error

	// Close tears down the connection and releases resources.
	Close() error

	// Tables returns metadata for tables to migrate. The table list is determined
	// by the config (explicit table list or auto-detect all tables).
	Tables(ctx context.Context) ([]TableInfo, error)

	// ReadBatch reads the next page of rows from the given table.
	// offset is a logical page number (0-based). Returns io.EOF when done.
	ReadBatch(ctx context.Context, table TableInfo, offset uint64) (RowBatch, error)

	// EstimateRowCount returns an approximate row count for progress reporting.
	EstimateRowCount(ctx context.Context, tableName string) (int64, error)
}

// TableInfo describes a table (or collection) to migrate.
type TableInfo struct {
	Schema       string       // Database/schema name
	Name         string       // Table/collection name
	Columns      []ColumnInfo // Column metadata
	PrimaryKey   []string     // Column names composing the PK
	EstimatedRows int64       // Approximate count, 0 if unknown
}

// ColumnInfo describes a single column.
type ColumnInfo struct {
	Name          string  // Column name
	OriginalType  string  // Native type string, e.g. "DATETIME(3)"
	CommonType    int     // CommonType enum value (from internal/schema)
	Nullable      bool
	Length        int     // For varchar, char
	Precision     int     // For decimal, numeric
	Scale         int     // For decimal, numeric
	AutoIncrement bool
	PrimaryKey    bool
	Default       *string // nil if no default
}

// RowBatch is a batch of rows returned by ReadBatch.
type RowBatch struct {
	Rows       [][]any // Each inner slice is one row, values in column order
	Offset     uint64  // The offset that was requested
	NextOffset uint64  // Cursor value for the next call (used for cursor-based pagination)
	TotalRows  *uint64 // Total rows in table (for progress), nil if unknown
	IsLast     bool    // True if this is the final batch
}

// Factory creates a new Source instance.
type Factory func() Source

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register registers a source factory. Call from init() in plugin packages.
// If a source with the same name is already registered, it is overwritten with a warning.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		slog.Warn("source already registered, overwriting", "name", name)
	}
	registry[name] = f
}

// Unregister removes a source factory. Safe to call on non-existent names.
func Unregister(name string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, name)
}

// Get returns the factory for a named source.
func Get(name string) (Factory, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSource, name)
	}
	return f, nil
}

// List returns all registered source names.
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

// Error constants for source operations.
var (
	ErrTableNotFound = errors.New("table not found")
)

// Ensure io import is used.
var _ = io.EOF
