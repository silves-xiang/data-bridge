// Package debug provides debug logging and diagnostics utilities.
package debug

import (
	"log/slog"
	"os"
)

// SetupDebug configures the global slog logger for debug mode.
func SetupDebug(enabled bool, verboseBatch bool, logMemory bool) {
	if !enabled {
		return
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))
	slog.Debug("debug mode enabled", "verbose_batch", verboseBatch, "log_memory", logMemory)
}
