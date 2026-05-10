// databridge migrates data between different databases.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/silves-xiang/data-bridge/internal/config"
	"github.com/silves-xiang/data-bridge/internal/pipeline"
	"github.com/silves-xiang/data-bridge/pkg/hook"
	"github.com/silves-xiang/data-bridge/pkg/plugin"
	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"

	// Register plugins via blank imports.
	_ "github.com/silves-xiang/data-bridge/plugins/clickhouse"
	_ "github.com/silves-xiang/data-bridge/plugins/hooks/exec"
	_ "github.com/silves-xiang/data-bridge/plugins/hooks/timescale"
	_ "github.com/silves-xiang/data-bridge/plugins/influxdb"
	_ "github.com/silves-xiang/data-bridge/plugins/kafka"
	_ "github.com/silves-xiang/data-bridge/plugins/mongodb"
	_ "github.com/silves-xiang/data-bridge/plugins/mysql"
	_ "github.com/silves-xiang/data-bridge/plugins/postgresql"
	_ "github.com/silves-xiang/data-bridge/plugins/redis"
)

var (
	version      = "0.1.0"
	cfgFile      string
	pluginLoader *plugin.Loader
)

func main() {
	root := &cobra.Command{
		Use:   "databridge",
		Short: "Migrate data between databases",
		Long: `databridge is a tool for migrating data between different database systems.
It supports MySQL, PostgreSQL, and more through a plugin architecture.

Configuration is driven by YAML files. Use 'databridge migrate -c config.yaml'
to run a migration.`,
		Version: version,
	}

	root.PersistentFlags().StringVarP(&cfgFile, "config", "c", "databridge.yaml", "config file path")

	root.AddCommand(migrateCmd())
	root.AddCommand(validateCmd())
	root.AddCommand(listCmd())
	root.AddCommand(versionCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// setupPlugins initializes the dynamic plugin loader and installs the SIGHUP handler.
func setupPlugins(pluginDir string) {
	pluginLoader = plugin.NewLoader(pluginDir)

	if err := pluginLoader.Load(); err != nil {
		slog.Warn("failed to load dynamic plugins", "error", err)
		return
	}

	loaded := pluginLoader.List()
	if len(loaded) > 0 {
		slog.Info("dynamic plugins loaded", "count", len(loaded), "dir", pluginDir)
	}

	// SIGHUP triggers plugin reload.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)
	go func() {
		for range sigCh {
			slog.Info("received SIGHUP, reloading plugins")
			if err := pluginLoader.Reload(); err != nil {
				slog.Warn("plugin reload failed", "error", err)
			} else {
				slog.Info("plugins reloaded", "count", len(pluginLoader.List()))
			}
		}
	}()
}

func migrateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run a migration",
		Long:  `Run a migration from source to sink database as defined in the config file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

				// Load dynamic plugins.
				setupPlugins(cfg.PluginDir)

			slog.Info("databridge starting",
				"task", cfg.Task.Name,
				"source", cfg.Source.Type,
				"sink", cfg.Sink.Type,
				"version", version,
				"go_version", runtime.Version(),
			)

			p, err := pipeline.New(cfg)
			if err != nil {
				return fmt.Errorf("create pipeline: %w", err)
			}

			ctx := cmd.Context()
			if err := p.Run(ctx); err != nil {
				return err
			}

			slog.Info("migration completed successfully")
			return nil
		},
	}

	return cmd
}

func validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate a config file without running",
		Long:  `Parse and validate the configuration file, reporting any errors.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}

			// Verify source connector exists.
			if _, err := source.Get(cfg.Source.Type); err != nil {
				return fmt.Errorf("source: %w", err)
			}

			// Verify sink connector exists.
			if _, err := sink.Get(cfg.Sink.Type); err != nil {
				return fmt.Errorf("sink: %w", err)
			}

			// Verify hooks.
			for _, hc := range cfg.Hooks {
				if _, err := hook.Get(hc.Type); err != nil {
					return fmt.Errorf("hook %q: %w", hc.Type, err)
				}
			}

			fmt.Println("Config is valid.")
			fmt.Printf("  Source: %s\n", cfg.Source.Type)
			fmt.Printf("  Sink:   %s\n", cfg.Sink.Type)
			fmt.Printf("  Tables: %d configured\n", len(cfg.Tables))
			fmt.Printf("  Hooks:  %d configured\n", len(cfg.Hooks))

			return nil
		},
	}
}

func listCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List available connectors and hooks",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Available sources:")
			for _, name := range source.List() {
				fmt.Printf("  - %s\n", name)
			}

			fmt.Println("\nAvailable sinks:")
			for _, name := range sink.List() {
				fmt.Printf("  - %s\n", name)
			}

			fmt.Println("\nAvailable hooks:")
			for _, name := range hook.List() {
				fmt.Printf("  - %s\n", name)
			}
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("databridge version %s\n", version)
			fmt.Printf("go version: %s\n", runtime.Version())
			fmt.Printf("os/arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		},
	}
}
