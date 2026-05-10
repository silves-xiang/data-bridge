package influxdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	api "github.com/influxdata/influxdb-client-go/v2/api"
	write "github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// influxExecutor implements sink.Executor as a no-op for InfluxDB.
type influxExecutor struct{}

func (e *influxExecutor) Exec(ctx context.Context, query string, args ...any) error {
	return nil
}

func (e *influxExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	return nil, fmt.Errorf("influxdb: Query not supported via Executor")
}

// SinkConfig holds additional sink parameters.
type SinkConfig struct {
	TimeColumn string
	TagColumns []string
}

// parseSinkConfig extracts sink-specific params.
func parseSinkConfig(params map[string]any) SinkConfig {
	sc := SinkConfig{}
	if v, ok := params["time_column"].(string); ok {
		sc.TimeColumn = v
	}
	if tags, ok := params["tag_columns"].([]any); ok {
		for _, t := range tags {
			if s, ok := t.(string); ok {
				sc.TagColumns = append(sc.TagColumns, s)
			}
		}
	}
	return sc
}

// Sink implements sink.Sink for InfluxDB.
type Sink struct {
	client    influxdb2.Client
	writeAPI  api.WriteAPI
	config    InfluxConnection
	sinkCfg   SinkConfig
	exec      *influxExecutor
}

// Open establishes an InfluxDB connection.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	// Extract sink-specific params (merged from ConnectorConfig.Params by pipeline).
	s.sinkCfg = parseSinkConfig(config)

	if s.config.URL == "" {
		return fmt.Errorf("influxdb: url is required")
	}
	if s.config.Token == "" {
		return fmt.Errorf("influxdb: token is required")
	}
	if s.config.Org == "" {
		return fmt.Errorf("influxdb: org is required")
	}
	if s.config.Bucket == "" {
		return fmt.Errorf("influxdb: bucket is required")
	}

	client := influxdb2.NewClient(s.config.URL, s.config.Token)

	ok, err := client.Ping(ctx)
	if err != nil {
		client.Close()
		return fmt.Errorf("influxdb ping: %w", err)
	}
	if !ok {
		client.Close()
		return fmt.Errorf("influxdb ping: unhealthy")
	}

	s.client = client
	s.writeAPI = client.WriteAPI(s.config.Org, s.config.Bucket)
	s.exec = &influxExecutor{}

	return nil
}

// Close closes the InfluxDB connection.
func (s *Sink) Close() error {
	if s.writeAPI != nil {
		s.writeAPI.Flush()
	}
	if s.client != nil {
		s.client.Close()
	}
	return nil
}

// Executor returns an executor (no-op for InfluxDB).
func (s *Sink) Executor() sink.Executor {
	return s.exec
}

// PrepareTarget is a no-op for the schemaless InfluxDB.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	return nil
}

// CleanupTarget is a no-op for the schemaless InfluxDB.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	return nil
}

// CreateTable is a no-op; InfluxDB measurements are created implicitly on write.
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	return nil
}

// WriteBatch writes a batch of rows as InfluxDB points using the async WriteAPI.
func (s *Sink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Build helpers for column lookup.
	colIdx := make(map[string]int)
	for i, col := range table.Columns {
		colIdx[col.Name] = i
	}

	tagSet := make(map[string]bool)
	for _, tc := range s.sinkCfg.TagColumns {
		tagSet[tc] = true
	}

	for _, row := range rows {
		point, err := s.buildPoint(table.Name, row, colIdx, tagSet)
		if err != nil {
			return 0, err
		}
		s.writeAPI.WritePoint(point)
	}

	s.writeAPI.Flush()

	// Check for async errors.
	select {
	case err := <-s.writeAPI.Errors():
		return 0, fmt.Errorf("write: %w", err)
	default:
	}

	return len(rows), nil
}

// buildPoint converts a row to an InfluxDB point.
func (s *Sink) buildPoint(measurement string, row []any, colIdx map[string]int, tagSet map[string]bool) (*write.Point, error) {
	tags := make(map[string]string)
	fields := make(map[string]any)
	var ptTime time.Time

	for colName, idx := range colIdx {
		if idx >= len(row) {
			continue
		}
		val := row[idx]

		// Handle time column.
		if colName == s.sinkCfg.TimeColumn {
			if t, ok := val.(time.Time); ok {
				ptTime = t
			} else if s, ok := val.(string); ok {
				t, err := time.Parse("2006-01-02 15:04:05.999999", s)
				if err == nil {
					ptTime = t
				}
			}
			continue
		}

		if val == nil {
			continue
		}

		// Skip _time metadata from source reads.
		if colName == "_time" {
			continue
		}

		if tagSet[colName] {
			tags[colName] = fmt.Sprintf("%v", val)
		} else {
			fields[colName] = normalizeFieldValue(val)
		}
	}

	if ptTime.IsZero() {
		ptTime = time.Now()
	}

	if len(tags) == 0 {
		return influxdb2.NewPoint(measurement, nil, fields, ptTime), nil
	}
	return influxdb2.NewPoint(measurement, tags, fields, ptTime), nil
}

// normalizeFieldValue converts a value to an InfluxDB-compatible field type.
func normalizeFieldValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case sql.NullString:
		if t.Valid {
			return t.String
		}
		return nil
	case sql.NullInt64:
		if t.Valid {
			return t.Int64
		}
		return nil
	case sql.NullFloat64:
		if t.Valid {
			return t.Float64
		}
		return nil
	case sql.NullBool:
		if t.Valid {
			return t.Bool
		}
		return nil
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case float32:
		return float64(t)
	case time.Time:
		return t.Format("2006-01-02 15:04:05.999999999")
	default:
		return v
	}
}

// Ensure write import is used.
var _ = write.Point{}
