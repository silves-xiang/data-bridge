package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// mongoExecutor implements sink.Executor as a no-op for MongoDB.
type mongoExecutor struct{}

func (e *mongoExecutor) Exec(ctx context.Context, query string, args ...any) error {
	return fmt.Errorf("mongodb: Exec not supported")
}

func (e *mongoExecutor) Query(ctx context.Context, query string, args ...any) (sink.Rows, error) {
	return nil, fmt.Errorf("mongodb: Query not supported")
}

// Sink implements sink.Sink for MongoDB.
type Sink struct {
	client *mongo.Client
	db     *mongo.Database
	config MongoConnection
	exec   *mongoExecutor
}

// Open establishes a MongoDB connection.
func (s *Sink) Open(ctx context.Context, config map[string]any) error {
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
	s.exec = &mongoExecutor{}
	return nil
}

// Close closes the MongoDB connection.
func (s *Sink) Close() error {
	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.client.Disconnect(ctx)
	}
	return nil
}

// Executor returns a no-op executor.
func (s *Sink) Executor() sink.Executor {
	return s.exec
}

// PrepareTarget is a no-op for MongoDB.
func (s *Sink) PrepareTarget(ctx context.Context, tables []source.TableInfo) error {
	return nil
}

// CleanupTarget is a no-op for MongoDB.
func (s *Sink) CleanupTarget(ctx context.Context) error {
	return nil
}

// CreateTable is a no-op for the schemaless MongoDB.
func (s *Sink) CreateTable(ctx context.Context, table source.TableInfo) error {
	return nil
}

// WriteBatch inserts a batch of documents using insertMany with unordered mode.
func (s *Sink) WriteBatch(ctx context.Context, table source.TableInfo, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	docs := make([]any, len(rows))
	for i, row := range rows {
		doc := bson.M{}
		for j, col := range table.Columns {
			if j < len(row) && row[j] != nil {
				doc[col.Name] = row[j]
			}
		}
		docs[i] = doc
	}

	coll := s.db.Collection(table.Name)
	opts := options.InsertMany().SetOrdered(false)
	_, err := coll.InsertMany(ctx, docs, opts)
	if err != nil {
		// Unordered insert ignores duplicate key errors.
		if bulkErr, ok := err.(mongo.BulkWriteException); ok {
			inserted := len(docs) - len(bulkErr.WriteErrors)
			return inserted, nil
		}
		return 0, fmt.Errorf("insert: %w", err)
	}

	return len(rows), nil
}
