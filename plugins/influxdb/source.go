package influxdb

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// InfluxConnection holds parsed connection parameters.
type InfluxConnection struct {
	URL    string
	Token  string
	Org    string
	Bucket string
}

// parseConnection extracts InfluxDB connection params from the config map.
func parseConnection(cfg map[string]any) InfluxConnection {
	c := InfluxConnection{}
	if v, ok := cfg["url"].(string); ok {
		c.URL = v
	}
	if v, ok := cfg["token"].(string); ok {
		c.Token = v
	}
	if v, ok := cfg["org"].(string); ok {
		c.Org = v
	}
	if v, ok := cfg["bucket"].(string); ok {
		c.Bucket = v
	}
	return c
}

// tableMeta stores per-measurement schema info for building Flux pivot queries.
type tableMeta struct {
	tagKeys   []string
	fieldKeys []string
}

// Source implements source.Source for InfluxDB.
type Source struct {
	client    influxdb2.Client
	config    InfluxConnection
	tableMeta map[string]tableMeta // measurement -> metadata
}

// Open establishes an InfluxDB connection.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

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
	s.tableMeta = make(map[string]tableMeta)
	return nil
}

// Close closes the InfluxDB connection.
func (s *Source) Close() error {
	if s.client != nil {
		s.client.Close()
	}
	return nil
}

// Tables discovers all measurements in the bucket.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	queryAPI := s.client.QueryAPI(s.config.Org)

	measQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.measurements(bucket: %q)`, s.config.Bucket)

	result, err := queryAPI.Query(ctx, measQuery)
	if err != nil {
		return nil, fmt.Errorf("list measurements: %w", err)
	}

	var tables []source.TableInfo
	for result.Next() {
		v := result.Record().ValueByKey("_value")
		if v == nil {
			continue
		}
		measName := fmt.Sprintf("%v", v)

		info, err := s.tableInfo(ctx, measName)
		if err != nil {
			return nil, fmt.Errorf("measurement %s: %w", measName, err)
		}
		tables = append(tables, info)
	}

	return tables, result.Err()
}

// tableInfo returns metadata for a single measurement.
func (s *Source) tableInfo(ctx context.Context, measName string) (source.TableInfo, error) {
	info := source.TableInfo{
		Schema: s.config.Bucket,
		Name:   measName,
	}

	queryAPI := s.client.QueryAPI(s.config.Org)

	// Discover tag keys (start: -100y to cover all historical data).
	tagQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.measurementTagKeys(bucket: %q, measurement: %q, start: -100y)`, s.config.Bucket, measName)

	tagResult, err := queryAPI.Query(ctx, tagQuery)
	if err != nil {
		return info, fmt.Errorf("tag keys: %w", err)
	}

	var tagKeys []string
	for tagResult.Next() {
		v := tagResult.Record().ValueByKey("_value")
		if v != nil {
			key := fmt.Sprintf("%v", v)
			// Filter out Flux internal columns that leak into schema results.
			if !isFluxInternal(key) {
				tagKeys = append(tagKeys, key)
			}
		}
	}

	// Discover field keys (start: -100y to cover all historical data).
	fieldQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.measurementFieldKeys(bucket: %q, measurement: %q, start: -100y)`, s.config.Bucket, measName)

	fieldResult, err := queryAPI.Query(ctx, fieldQuery)
	if err != nil {
		return info, fmt.Errorf("field keys: %w", err)
	}

	var fieldKeys []string
	for fieldResult.Next() {
		v := fieldResult.Record().ValueByKey("_value")
		if v != nil {
			key := fmt.Sprintf("%v", v)
			if !isFluxInternal(key) {
				fieldKeys = append(fieldKeys, key)
			}
		}
	}

	// Store metadata for ReadBatch.
	s.tableMeta[measName] = tableMeta{
		tagKeys:   tagKeys,
		fieldKeys: fieldKeys,
	}

	// Build columns: _time + tag keys + field keys.
	info.Columns = append(info.Columns, source.ColumnInfo{
		Name:         "_time",
		OriginalType: "dateTime:RFC3339",
		CommonType:   int(schema.TypeTimestamp),
	})

	for _, tk := range tagKeys {
		info.Columns = append(info.Columns, source.ColumnInfo{
			Name:         tk,
			OriginalType: "string",
			CommonType:   int(schema.TypeString),
		})
	}

	for _, fk := range fieldKeys {
		info.Columns = append(info.Columns, source.ColumnInfo{
			Name:         fk,
			OriginalType: "string", // schemaless — typed at read time
			CommonType:   int(schema.TypeString),
		})
	}

	return info, nil
}

// EstimateRowCount returns 0; counting is expensive in InfluxDB.
func (s *Source) EstimateRowCount(ctx context.Context, tableName string) (int64, error) {
	return 0, nil
}

// ReadBatch reads a page of rows using Flux pivot + limit/offset pagination.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	const batchSize = 1000
	queryAPI := s.client.QueryAPI(s.config.Org)

	meta, ok := s.tableMeta[table.Name]
	if !ok {
		return source.RowBatch{}, fmt.Errorf("influxdb: no metadata for measurement %q", table.Name)
	}

	fluxQuery := buildReadQuery(s.config.Bucket, table.Name, meta.tagKeys, meta.fieldKeys, int(offset)*batchSize, batchSize)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return source.RowBatch{}, fmt.Errorf("query: %w", err)
	}

	batch := source.RowBatch{Offset: offset}

	// Build a lookup from column name to position.
	colIdx := make(map[string]int)
	for i, col := range table.Columns {
		colIdx[col.Name] = i
	}

	for result.Next() {
		record := result.Record()
		rowTime := record.Time()
		values := record.Values()

		row := make([]any, len(table.Columns))

		// Set _time.
		if idx, ok := colIdx["_time"]; ok {
			row[idx] = rowTime.Format("2006-01-02 15:04:05.999999999")
		}

		// Set tag and field values.
		for colName, val := range values {
			// Skip metadata columns.
			if colName == "_start" || colName == "_stop" || colName == "_measurement" ||
				colName == "result" || colName == "table" || colName == "_field" || colName == "_value" {
				continue
			}
			if idx, ok := colIdx[colName]; ok {
				row[idx] = normalizeValue(val)
			}
		}

		batch.Rows = append(batch.Rows, row)
	}

	if err := result.Err(); err != nil {
		return batch, fmt.Errorf("result iteration: %w", err)
	}

	if len(batch.Rows) < batchSize {
		batch.IsLast = true
	}

	if len(batch.Rows) == 0 && offset > 0 {
		return source.RowBatch{}, io.EOF
	}

	return batch, nil
}

// buildReadQuery constructs a Flux query for paginated, pivoted reads.
func buildReadQuery(bucket, measurement string, tagKeys, fieldKeys []string, skip, limit int) string {
	// Build pivot rowKey: _time + all tag keys.
	rowKey := make([]string, 1, 1+len(tagKeys))
	rowKey[0] = "_time"
	rowKey = append(rowKey, tagKeys...)

	rowKeyParts := make([]string, len(rowKey))
	for i, k := range rowKey {
		rowKeyParts[i] = fmt.Sprintf("%q", k)
	}

	return fmt.Sprintf(
		`from(bucket: %q)
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == %q)
  |> pivot(rowKey: [%s], columnKey: ["_field"], valueColumn: "_value")
  |> limit(n: %d, offset: %d)
  |> group()`,
		bucket, measurement,
		strings.Join(rowKeyParts, ", "),
		limit, skip,
	)
}

// normalizeValue converts InfluxDB-returned values to portable string types.
// Numeric and bool values are converted to strings because InfluxDB fields are
// schemaless and the column type is declared as TypeString.
func normalizeValue(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(t)
	case string:
		return t
	case time.Time:
		return t.Format("2006-01-02 15:04:05.999999999")
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(t, 10)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", t)
	}
}

// isFluxInternal returns true for Flux metadata column names that leak into
// schema queries (measurementTagKeys / measurementFieldKeys results).
func isFluxInternal(name string) bool {
	switch name {
	case "_start", "_stop", "_field", "_measurement", "_value":
		return true
	}
	return false
}
