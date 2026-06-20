package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Checkpoint supports pause/resume of migrations.
// It tracks per-table offset and completion status in a JSON file.
type Checkpoint struct {
	mu       sync.Mutex
	TaskName string            `json:"task_name"`
	Dir      string            `json:"-"`
	Tables   map[string]*TableCheckpoint `json:"tables"`
}

// TableCheckpoint tracks progress for a single table.
type TableCheckpoint struct {
	Name       string `json:"name"`
	Offset     uint64 `json:"offset"`      // Next page to read
	Completed  bool   `json:"completed"`
	RowsCopied uint64 `json:"rows_copied"`
}

// NewCheckpoint creates a new checkpoint for the given task.
func NewCheckpoint(taskName, dir string) *Checkpoint {
	return &Checkpoint{
		TaskName: taskName,
		Dir:      dir,
		Tables:   make(map[string]*TableCheckpoint),
	}
}

// Load reads the checkpoint from disk. Returns a fresh checkpoint if not found.
func (c *Checkpoint) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.filePath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	return json.Unmarshal(data, c)
}

// Save writes the checkpoint to disk.
// Holds the lock for the entire operation to prevent concurrent saves
// from overwriting each other with stale data.
func (c *Checkpoint) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(c.Dir, 0755); err != nil {
		return err
	}

	tmpPath := c.filePath() + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, c.filePath())
}

// GetOffset returns the next page offset for a table. Returns 0 if not started.
func (c *Checkpoint) GetOffset(tableName string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	tc, ok := c.Tables[tableName]
	if !ok {
		return 0
	}
	return tc.Offset
}

// SetOffset updates the offset for a table.
func (c *Checkpoint) SetOffset(tableName string, offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Tables[tableName] == nil {
		c.Tables[tableName] = &TableCheckpoint{Name: tableName}
	}
	c.Tables[tableName].Offset = offset
}

// AddRows adds to the rows_copied counter for a table.
func (c *Checkpoint) AddRows(tableName string, count uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Tables[tableName] == nil {
		c.Tables[tableName] = &TableCheckpoint{Name: tableName}
	}
	c.Tables[tableName].RowsCopied += count
}

// MarkComplete marks a table as fully migrated.
func (c *Checkpoint) MarkComplete(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Tables[tableName] == nil {
		c.Tables[tableName] = &TableCheckpoint{Name: tableName}
	}
	c.Tables[tableName].Completed = true
}

// IsComplete returns whether a table has been fully migrated.
func (c *Checkpoint) IsComplete(tableName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	tc, ok := c.Tables[tableName]
	if !ok {
		return false
	}
	return tc.Completed
}

// filePath returns the checkpoint file path.
func (c *Checkpoint) filePath() string {
	return filepath.Join(c.Dir, c.TaskName+".checkpoint.json")
}
