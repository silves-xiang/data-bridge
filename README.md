English | [简体中文](README_zh.md)

# databridge

Migrate data between different databases with ease.

## Features

- **Plugin architecture** — Add new databases by implementing interfaces, no core changes needed
- **Dynamic plugin loading** — Load `.so` plugins at runtime (Go `plugin` package), hot-reload with SIGHUP
- **Streaming migration** — Batch-based processing, won't OOM on large tables
- **Checkpoint & resume** — Interrupted migrations can continue from where they left off
- **Parallel table migration** — Multiple tables migrated concurrently with configurable parallelism
- **Lifecycle hooks** — Inject custom logic at pipeline/table/batch stages (e.g., TimescaleDB hypertable setup)
- **Schema mapping** — Automatic type conversion between source and sink databases
- **Debug & diagnostics** — Optional pprof profile collection for performance analysis

## Supported Databases

| Database | Source | Sink |
|----------|--------|------|
| MySQL | ✓ | ✓ |
| PostgreSQL | ✓ | ✓ |
| InfluxDB | ✓ | ✓ |

More coming: MongoDB, Redis, Kafka, ClickHouse.

## Quick Start

### Install

```bash
go install github.com/silves-xiang/data-bridge/cmd/databridge@latest
```

### Usage

```bash
# Run a migration
databridge migrate -c config.yaml

# Validate config without running
databridge validate -c config.yaml

# List available connectors and hooks
databridge list

# Show version
databridge version
```

### Configuration

Create a YAML config file:

```yaml
task:
  name: "my-migration"
  mode: full

source:
  type: mysql
  connection:
    host: "127.0.0.1"
    port: 3306
    user: "root"
    password: "${MYSQL_PASSWORD}"
    database: "source_db"

sink:
  type: postgresql
  connection:
    host: "127.0.0.1"
    port: 5432
    user: "postgres"
    password: "${PG_PASSWORD}"
    database: "target_db"
    ssl_mode: "disable"

tables:
  - source: "users"
    target: "users"
    batch_size: 5000

parallelism: 4

checkpoint:
  enabled: true
  dir: "./.databridge/checkpoints"
```

### InfluxDB Configuration

InfluxDB can be used as both source and sink. As a sink, you can configure which
columns become tags and which column provides the timestamp:

```yaml
source:
  type: influxdb
  connection:
    url: "http://localhost:8086"
    token: "${INFLUXDB_TOKEN}"
    org: "myorg"
    bucket: "mybucket"

sink:
  type: influxdb
  connection:
    url: "http://localhost:8086"
    token: "${INFLUXDB_TOKEN}"
    org: "myorg"
    bucket: "target_bucket"
  params:
    time_column: "created_at"     # source column to use as timestamp
    tag_columns: ["sensor_id"]    # source columns to store as tags
```

See [examples/](examples/) for full configuration examples.

## Architecture

```
Source (MySQL)  ──ReadBatch──>  Pipeline  ──WriteBatch──>  Sink (PostgreSQL)
                                    │
                              ┌─────┼─────┐
                              │     │     │
                         Checkpoint Hooks  Worker Pool
```

### Core Interfaces

- **Source** — Reads tables and row batches from a source database
- **Sink** — Creates tables and writes row batches to a target database
- **Hook** — Lifecycle callbacks: `PipelineHook`, `TableHook`, `BatchHook`

### Adding a New Database

**Compile-time (built-in):**

1. Implement `source.Source` and/or `sink.Sink` interfaces
2. Implement schema mapping (`SourceTypeMapper` / `TargetTypeMapper`)
3. Register in `init()` via `source.Register("name", factory)` / `sink.Register("name", factory)`
4. Import the plugin package in `cmd/databridge/main.go`

**Runtime (.so dynamic loading):**

Plugins can be compiled as shared objects and loaded at runtime without
recompiling the main binary. Each `.so` must export a `Register` function:

```go
package main

import _ "github.com/silves-xiang/data-bridge/plugins/myplugin"

func Register() {}
```

Build with:

```bash
make plugin-myplugin    # produces plugins/myplugin.so
```

Set `plugin_dir` in your config, and plugins are loaded at startup:

```yaml
plugin_dir: "./plugins"
```

To hot-reload plugins after adding or removing `.so` files:

```bash
kill -SIGHUP $(pgrep databridge)
```

Note: Go `plugin` requires the plugin and main binary to use the same Go version.
Only supported on Linux, FreeBSD, and macOS.

## Hooks

Hooks allow custom logic at migration lifecycle points:

- **PipelineHook** — `OnPipelineStart` / `OnPipelineEnd`
- **TableHook** — `OnTableStart` / `OnTableEnd` (e.g., create TimescaleDB hypertable)
- **BatchHook** — `OnBatchComplete` (e.g., periodic aggregation)

```yaml
hooks:
  - name: "create-hypertables"
    type: "timescale"
    params:
      partition_column: "created_at"
      hypertable_interval: "7 days"
      enable_compression: true
      compression_after: "30 days"
```

## Debug & Diagnostics

```yaml
debug:
  enabled: true
  verbose_batch: true    # Log every batch timing and row count
  log_memory: true       # Log memory usage per batch

pprof:
  enabled: true
  dir: "./.databridge/pprof"
  interval: "5m"         # Capture interval
  profiles:
    - "heap"
    - "goroutine"
    - "allocs"
  cpu_duration: "30s"
```

Analyze profiles with:
```bash
go tool pprof -http=:8080 .databridge/pprof/heap_20260101_120000.prof
```

## License

MIT
