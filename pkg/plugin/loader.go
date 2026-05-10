// Package plugin provides dynamic loading of Go plugins (.so files) at runtime.
// It scans a directory for .so files built with -buildmode=plugin and calls each
// plugin's exported Register function to populate the global source/sink/hook registries.
package plugin

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	stdplugin "plugin"
	"sort"
	"sync"

	"github.com/silves-xiang/data-bridge/pkg/hook"
	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// Loader manages dynamic plugin .so files.
type Loader struct {
	mu     sync.Mutex
	dir    string
	// plugins maps filename -> plugin handle (for reload tracking).
	plugins map[string]*stdplugin.Plugin
	// names tracks which registry names were registered by dynamic plugins,
	// so they can be cleaned up on reload.
	names []string
}

// NewLoader creates a plugin loader for the given directory.
func NewLoader(dir string) *Loader {
	return &Loader{
		dir:     dir,
		plugins: make(map[string]*stdplugin.Plugin),
	}
}

// Load scans the plugin directory and loads all .so files.
// Skips already-loaded files. Errors loading individual files are logged
// and skipped; the caller can inspect which files failed.
func (l *Loader) Load() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	entries, err := os.ReadDir(l.dir)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Debug("plugin directory does not exist, skipping", "dir", l.dir)
			return nil
		}
		return fmt.Errorf("read plugin dir %s: %w", l.dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".so" {
			continue
		}

		if _, loaded := l.plugins[entry.Name()]; loaded {
			continue
		}

		l.loadOne(entry.Name())
	}

	return nil
}

// Reload re-scans the plugin directory. Intended for SIGHUP handling.
// Dynamically registered plugins are unregistered, and the directory is
// re-scanned. Go cannot unload .so files from memory, so old plugins
// remain resident but their registry entries are replaced.
func (l *Loader) Reload() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Unregister all previously loaded dynamic plugins.
	for _, name := range l.names {
		source.Unregister(name)
		sink.Unregister(name)
		hook.Unregister(name)
	}
	l.names = nil
	l.plugins = make(map[string]*stdplugin.Plugin)

	entries, err := os.ReadDir(l.dir)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("plugin directory not found during reload", "dir", l.dir)
			return nil
		}
		return fmt.Errorf("read plugin dir %s: %w", l.dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".so" {
			continue
		}
		l.loadOne(entry.Name())
	}

	return nil
}

// loadOne loads a single .so file. Must be called with l.mu held.
func (l *Loader) loadOne(filename string) {
	path := filepath.Join(l.dir, filename)

	p, err := stdplugin.Open(path)
	if err != nil {
		slog.Warn("failed to open plugin", "file", filename, "error", err)
		return
	}

	sym, err := p.Lookup("Register")
	if err != nil {
		slog.Warn("plugin has no Register symbol", "file", filename, "error", err)
		return
	}

	register, ok := sym.(func())
	if !ok {
		slog.Warn("plugin Register is not func()", "file", filename)
		return
	}

	// Capture which registry names exist before Register runs.
	beforeSources := setOf(source.List())
	beforeSinks := setOf(sink.List())
	beforeHooks := setOf(hook.List())

	register()

	// Detect which names this plugin registered.
	for _, n := range source.List() {
		if !beforeSources[n] {
			l.names = append(l.names, n)
		}
	}
	for _, n := range sink.List() {
		if !beforeSinks[n] {
			l.names = append(l.names, n)
		}
	}
	for _, n := range hook.List() {
		if !beforeHooks[n] {
			l.names = append(l.names, n)
		}
	}

	l.plugins[filename] = p
	slog.Info("loaded plugin", "file", filename)
}

// List returns the filenames of currently loaded plugins.
func (l *Loader) List() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.plugins))
	for n := range l.plugins {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Dir returns the configured plugin directory.
func (l *Loader) Dir() string {
	return l.dir
}

func setOf(slice []string) map[string]bool {
	m := make(map[string]bool, len(slice))
	for _, s := range slice {
		m[s] = true
	}
	return m
}
