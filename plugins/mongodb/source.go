package mongodb

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MongoConnection holds parsed connection parameters.
type MongoConnection struct {
	URI      string
	Database string
}

// parseConnection extracts MongoDB connection params from config map.
func parseConnection(cfg map[string]any) MongoConnection {
	c := MongoConnection{}
	if v, ok := cfg["uri"].(string); ok {
		c.URI = v
	}
	if v, ok := cfg["database"].(string); ok {
		c.Database = v
	}
	return c
}

// Source implements source.Source for MongoDB.
type Source struct {
	client *mongo.Client
	db     *mongo.Database
	config MongoConnection
}

// Open establishes a MongoDB connection.
func (s *Source) Open(ctx context.Context, config map[string]any) error {
	s.config = parseConnection(config)

	if s.config.URI == "" {
		return fmt.Errorf("mongodb: uri is required")
	}
	if s.config.Database == "" {
		return fmt.Errorf("mongodb: database is required")
	}

	clientOpts := options.Client().ApplyURI(s.config.URI).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("mongodb connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return fmt.Errorf("mongodb ping: %w", err)
	}

	s.client = client
	s.db = client.Database(s.config.Database)
	return nil
}

// Close closes the MongoDB connection.
func (s *Source) Close() error {
	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.client.Disconnect(ctx)
	}
	return nil
}

// Tables discovers all collections in the database.
func (s *Source) Tables(ctx context.Context) ([]source.TableInfo, error) {
	names, err := s.db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}

	var tables []source.TableInfo
	for _, name := range names {
		// Skip system collections.
		if len(name) >= 7 && name[:7] == "system." {
			continue
		}
		info, err := s.tableInfo(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("collection %s: %w", name, err)
		}
		tables = append(tables, info)
	}
	return tables, nil
}

// tableInfo samples documents to infer the schema.
func (s *Source) tableInfo(ctx context.Context, collName string) (source.TableInfo, error) {
	info := source.TableInfo{
		Schema: s.config.Database,
		Name:   collName,
	}

	coll := s.db.Collection(collName)

	// Sample up to 100 documents to infer schema.
	cur, err := coll.Find(ctx, bson.D{}, options.Find().SetLimit(100))
	if err != nil {
		return info, fmt.Errorf("sample docs: %w", err)
	}
	defer cur.Close(ctx)

	seenCols := make(map[string]string)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		for key, val := range doc {
			if _, ok := seenCols[key]; !ok {
				seenCols[key] = bsonType(val)
			}
		}
	}

	// _id is always the primary key.
	info.PrimaryKey = []string{"_id"}

	for key, bsonTyp := range seenCols {
		commonType, _, _, _, err := MapSourceType(bsonTyp)
		if err != nil {
			commonType = schema.TypeString
		}
		col := source.ColumnInfo{
			Name:         key,
			OriginalType: bsonTyp,
			CommonType:   int(commonType),
			Nullable:     true,
			PrimaryKey:   key == "_id",
		}
		info.Columns = append(info.Columns, col)
	}

	// Ensure _id is first.
	for i, col := range info.Columns {
		if col.Name == "_id" {
			info.Columns[0], info.Columns[i] = info.Columns[i], info.Columns[0]
			break
		}
	}

	return info, cur.Err()
}

// bsonType returns a string representation of a BSON value's type.
func bsonType(v any) string {
	switch v.(type) {
	case float64:
		return "double"
	case string:
		return "string"
	case bson.M, bson.D:
		return "object"
	case bson.A, []any:
		return "array"
	case primitive.ObjectID, [12]byte:
		return "objectId"
	case bool:
		return "bool"
	case time.Time:
		return "date"
	case int32:
		return "int"
	case int64:
		return "long"
	case nil:
		return "null"
	default:
		return "string"
	}
}

// EstimateRowCount returns the estimated document count.
func (s *Source) EstimateRowCount(ctx context.Context, tableName string) (int64, error) {
	coll := s.db.Collection(tableName)
	count, err := coll.EstimatedDocumentCount(ctx)
	return count, err
}

// ReadBatch reads a page of documents using skip/limit.
func (s *Source) ReadBatch(ctx context.Context, table source.TableInfo, offset uint64) (source.RowBatch, error) {
	batchSize := int64(1000)
	coll := s.db.Collection(table.Name)

	opts := options.Find().
		SetSkip(int64(offset) * batchSize).
		SetLimit(batchSize)

	cur, err := coll.Find(ctx, bson.D{}, opts)
	if err != nil {
		return source.RowBatch{}, fmt.Errorf("find: %w", err)
	}
	defer cur.Close(ctx)

	batch := source.RowBatch{Offset: offset}

	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return batch, fmt.Errorf("decode: %w", err)
		}

		row := make([]any, len(table.Columns))
		for i, col := range table.Columns {
			val, ok := doc[col.Name]
			if !ok {
				row[i] = nil
				continue
			}
			row[i] = normalizeDocValue(val)
		}
		batch.Rows = append(batch.Rows, row)
	}

	if err := cur.Err(); err != nil {
		return batch, fmt.Errorf("cursor: %w", err)
	}

	if len(batch.Rows) < int(batchSize) {
		batch.IsLast = true
	}

	if len(batch.Rows) == 0 && offset > 0 {
		return source.RowBatch{}, io.EOF
	}

	return batch, nil
}

// normalizeDocValue converts BSON values to portable types.
func normalizeDocValue(v any) any {
	switch t := v.(type) {
	case primitive.ObjectID:
		return t.Hex()
	case time.Time:
		return t.Format("2006-01-02 15:04:05.999999999")
	case bson.M, bson.D, bson.A, []any:
		// Serialize complex types as JSON string for portability.
		return fmt.Sprintf("%v", t)
	default:
		return v
	}
}
