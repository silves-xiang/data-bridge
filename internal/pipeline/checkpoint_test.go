package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointSaveLoad(t *testing.T) {
	dir := t.TempDir()
	ckpt := NewCheckpoint("test-task", dir)

	ckpt.SetOffset("users", 5)
	ckpt.AddRows("users", 1000)
	ckpt.MarkComplete("orders")

	if err := ckpt.Save(); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	// Load into a new checkpoint.
	ckpt2 := NewCheckpoint("test-task", dir)
	if err := ckpt2.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if off := ckpt2.GetOffset("users"); off != 5 {
		t.Errorf("offset = %d, want 5", off)
	}
	if !ckpt2.IsComplete("orders") {
		t.Error("orders should be marked complete")
	}
	if ckpt2.IsComplete("users") {
		t.Error("users should NOT be marked complete")
	}
}

func TestCheckpointAtomicSave(t *testing.T) {
	dir := t.TempDir()
	ckpt := NewCheckpoint("test-task", dir)

	ckpt.SetOffset("users", 3)
	ckpt.Save()

	// Verify no .tmp file remains.
	tmpPath := filepath.Join(dir, "test-task.checkpoint.json.tmp")
	if exists(tmpPath) {
		t.Error(".tmp file should not exist after successful save")
	}

	// Verify the real file exists.
	realPath := filepath.Join(dir, "test-task.checkpoint.json")
	if !exists(realPath) {
		t.Error("checkpoint file should exist after save")
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
