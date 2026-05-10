package debug

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

// PprofConfig holds the configuration for pprof collection.
type PprofConfig struct {
	Enabled     bool
	Dir         string
	Interval    time.Duration
	Profiles    []string // heap, goroutine, allocs, cpu
	CPUDuration time.Duration
}

// StartPprof starts periodic pprof profile collection. Returns a stop function.
// If config is disabled, returns nil, nil.
func StartPprof(cfg PprofConfig) (func(), error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("create pprof dir: %w", err)
	}

	done := make(chan struct{})
	ticker := time.NewTicker(cfg.Interval)

	// Save initial heap profile.
	saveProfile(cfg.Dir, cfg.Profiles, cfg.CPUDuration)

	go func() {
		for {
			select {
			case <-ticker.C:
				saveProfile(cfg.Dir, cfg.Profiles, cfg.CPUDuration)
			case <-done:
				ticker.Stop()
				// Save final profiles.
				saveProfile(cfg.Dir, cfg.Profiles, cfg.CPUDuration)
				return
			}
		}
	}()

	slog.Info("pprof collection started",
		"dir", cfg.Dir,
		"interval", cfg.Interval,
		"profiles", cfg.Profiles,
	)

	return func() {
		close(done)
		slog.Info("pprof collection stopped")
	}, nil
}

// saveProfile writes the requested profiles to disk.
func saveProfile(dir string, profiles []string, cpuDuration time.Duration) {
	timestamp := time.Now().Format("20060102_150405")

	for _, name := range profiles {
		path := filepath.Join(dir, fmt.Sprintf("%s_%s.prof", name, timestamp))

		switch name {
		case "heap":
			f, err := os.Create(path)
			if err != nil {
				slog.Warn("pprof: create heap profile", "error", err)
				continue
			}
			runtime.GC() // Run GC to get a more accurate heap profile.
			if err := pprof.WriteHeapProfile(f); err != nil {
				slog.Warn("pprof: write heap profile", "error", err)
			}
			f.Close()

		case "goroutine":
			f, err := os.Create(path)
			if err != nil {
				slog.Warn("pprof: create goroutine profile", "error", err)
				continue
			}
			if err := pprof.Lookup("goroutine").WriteTo(f, 0); err != nil {
				slog.Warn("pprof: write goroutine profile", "error", err)
			}
			f.Close()

		case "allocs":
			f, err := os.Create(path)
			if err != nil {
				slog.Warn("pprof: create allocs profile", "error", err)
				continue
			}
			if err := pprof.Lookup("allocs").WriteTo(f, 1); err != nil {
				slog.Warn("pprof: write allocs profile", "error", err)
			}
			f.Close()

		case "cpu":
			if cpuDuration <= 0 {
				continue
			}
			f, err := os.Create(path)
			if err != nil {
				slog.Warn("pprof: create cpu profile", "error", err)
				continue
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				slog.Warn("pprof: start cpu profile", "error", err)
				f.Close()
				continue
			}
			time.Sleep(cpuDuration)
			pprof.StopCPUProfile()
			f.Close()
		}
	}
}
