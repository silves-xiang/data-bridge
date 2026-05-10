package testutil

import (
	"context"
	"io"

	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MockSource implements source.Source for testing.
type MockSource struct {
	OpenFunc             func(ctx context.Context, config map[string]any) error
	CloseFunc            func() error
	TablesFunc           func(ctx context.Context) ([]source.TableInfo, error)
	ReadBatchFunc        func(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error)
	EstimateRowCountFunc func(ctx context.Context, tableName string) (int64, error)
}

func (m *MockSource) Open(ctx context.Context, config map[string]any) error {
	if m.OpenFunc != nil {
		return m.OpenFunc(ctx, config)
	}
	return nil
}

func (m *MockSource) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func (m *MockSource) Tables(ctx context.Context) ([]source.TableInfo, error) {
	if m.TablesFunc != nil {
		return m.TablesFunc(ctx)
	}
	return nil, nil
}

func (m *MockSource) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	if m.ReadBatchFunc != nil {
		return m.ReadBatchFunc(ctx, table, offset)
	}
	return source.RowBatch{}, io.EOF
}

func (m *MockSource) EstimateRowCount(ctx context.Context, tableName string) (int64, error) {
	if m.EstimateRowCountFunc != nil {
		return m.EstimateRowCountFunc(ctx, tableName)
	}
	return 0, nil
}

var _ source.Source = (*MockSource)(nil)

// NewMockSource creates a simple mock with a single table and row data.
func NewMockSource(rows [][]any) *MockSource {
	offset := 0
	return &MockSource{
		TablesFunc: func(ctx context.Context) ([]source.TableInfo, error) {
			return []source.TableInfo{
				{
					Name:    "test_table",
					Columns: []source.ColumnInfo{
						{Name: "id", CommonType: 5},   // TypeInt64
						{Name: "name", CommonType: 15}, // TypeString
					},
					PrimaryKey: []string{"id"},
				},
			}, nil
		},
		ReadBatchFunc: func(ctx context.Context, table source.TableInfo, off uint64) (source.RowBatch, error) {
			if offset >= len(rows) {
				return source.RowBatch{}, io.EOF
			}
			batch := source.RowBatch{
				Rows: rows[offset:min(int(offset)+1, len(rows))],
			}
			offset++
			if offset >= len(rows) {
				batch.IsLast = true
			}
			return batch, nil
		},
		EstimateRowCountFunc: func(ctx context.Context, tableName string) (int64, error) {
			return int64(len(rows)), nil
		},
	}
}
