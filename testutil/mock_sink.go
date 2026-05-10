package testutil

import (
	"context"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MockSink implements sink.Sink for testing.
type MockSink struct {
	OpenFunc          func(ctx context.Context, config map[string]any) error
	CloseFunc         func() error
	ExecutorFunc      func() sink.Executor
	CreateTableFunc   func(ctx context.Context, table source.TableInfo) error
	WriteBatchFunc    func(ctx context.Context, table source.TableInfo, rows [][]any) (int, error)
	PrepareTargetFunc func(ctx context.Context, tables []source.TableInfo) error
	CleanupTargetFunc func(ctx context.Context) error

	// Captured rows for assertion.
	WrittenRows [][]any
}

func (m *MockSink) Open(ctx context.Context, config map[string]any) error {
	if m.OpenFunc != nil {
		return m.OpenFunc(ctx, config)
	}
	return nil
}

func (m *MockSink) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func (m *MockSink) Executor() sink.Executor {
	if m.ExecutorFunc != nil {
		return m.ExecutorFunc()
	}
	return &MockExecutor{}
}

func (m *MockSink) CreateTable(ctx context.Context, table source.TableInfo) error {
	if m.CreateTableFunc != nil {
		return m.CreateTableFunc(ctx, table)
	}
	return nil
}

func (m *MockSink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	m.WrittenRows = append(m.WrittenRows, rows...)
	if m.WriteBatchFunc != nil {
		return m.WriteBatchFunc(ctx, table, rows)
	}
	return len(rows), nil
}

func (m *MockSink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	if m.PrepareTargetFunc != nil {
		return m.PrepareTargetFunc(ctx, tables)
	}
	return nil
}

func (m *MockSink) CleanupTarget(ctx context.Context) error {
	if m.CleanupTargetFunc != nil {
		return m.CleanupTargetFunc(ctx)
	}
	return nil
}

var _ sink.Sink = (*MockSink)(nil)

// MockExecutor implements sink.Executor for testing.
type MockExecutor struct {
	ExecFunc  func(ctx context.Context, query string, args ...any) error
	QueryFunc func(ctx context.Context, query string, args ...any) (sink.Rows, error)
}

func (e *MockExecutor) Exec(ctx context.Context, query string, args ...any) error {
	if e.ExecFunc != nil {
		return e.ExecFunc(ctx, query, args...)
	}
	return nil
}

func (e *MockExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	if e.QueryFunc != nil {
		return e.QueryFunc(ctx, query, args...)
	}
	return nil, nil
}

var _ sink.Executor = (*MockExecutor)(nil)
