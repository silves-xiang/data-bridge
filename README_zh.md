[English](README.md) | 简体中文

# databridge

在不同数据库之间轻松迁移数据。

## 特性

- **插件化架构** — 新增数据库只需实现接口，无需修改核心代码
- **动态插件加载** — 运行时加载 `.so` 插件（Go `plugin` 包），支持 SIGHUP 热重载
- **流式迁移** — 基于批次处理，大表不会 OOM
- **断点续传** — 中断的迁移可以从断点继续，无需重新开始
- **并行迁移** — 多张表可配置并发数同时迁移
- **生命周期 Hook** — 在 Pipeline/表/批次阶段注入自定义逻辑（如自动创建 TimescaleDB  hypertable）
- **Schema 映射** — 源端和目标端数据库之间自动类型转换
- **Debug 诊断** — 可选的 pprof profile 采集，用于排查性能瓶颈和内存泄漏

## 支持的数据库

| 数据库 | 作为源 | 作为目标 |
|--------|--------|----------|
| MySQL | ✓ | ✓ |
| PostgreSQL | ✓ | ✓ |
| InfluxDB | ✓ | ✓ |

即将支持：MongoDB、Redis、Kafka、ClickHouse。

## 快速开始

### 安装

```bash
go install github.com/silves-xiang/data-bridge/cmd/databridge@latest
```

### 使用

```bash
# 运行迁移
databridge migrate -c config.yaml

# 验证配置文件（不执行迁移）
databridge validate -c config.yaml

# 列出可用的连接器和 Hook
databridge list

# 查看版本
databridge version
```

### 配置文件

创建一个 YAML 配置文件：

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

### InfluxDB 配置

InfluxDB 可作为源或目标。作为目标时，可通过 `params` 指定哪些列作为 tag 以及时间戳列：

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
    time_column: "created_at"     # 作为时间戳的源列
    tag_columns: ["sensor_id"]    # 作为 tag 存储的源列
```

完整配置示例见 [examples/](examples/)。

## 架构

```
Source (MySQL)  ──ReadBatch──>  Pipeline  ──WriteBatch──>  Sink (PostgreSQL)
                                    │
                              ┌─────┼─────┐
                              │     │     │
                         Checkpoint Hooks  Worker Pool
```

### 核心接口

- **Source** — 从源数据库读取表结构和行数据
- **Sink** — 在目标数据库建表并写入行数据
- **Hook** — 生命周期回调：`PipelineHook`、`TableHook`、`BatchHook`

### 添加新数据库

**编译时（内置）：**

1. 实现 `source.Source` 和/或 `sink.Sink` 接口
2. 实现 Schema 映射（`SourceTypeMapper` / `TargetTypeMapper`）
3. 在 `init()` 中注册：`source.Register("名称", factory)` / `sink.Register("名称", factory)`
4. 在 `cmd/databridge/main.go` 中 import 插件包

**运行时（.so 动态加载）：**

插件可编译为共享对象，运行时加载无需重新编译主程序。每个 `.so` 需导出 `Register` 函数：

```go
package main

import _ "github.com/silves-xiang/data-bridge/plugins/myplugin"

func Register() {}
```

构建命令：

```bash
make plugin-myplugin    # 生成 plugins/myplugin.so
```

在配置中设置 `plugin_dir`，启动时自动加载：

```yaml
plugin_dir: "./plugins"
```

添加或移除 `.so` 文件后，发送 SIGHUP 热重载：

```bash
kill -SIGHUP $(pgrep databridge)
```

注意：Go `plugin` 要求插件与主程序使用相同 Go 版本，仅支持 Linux、FreeBSD 和 macOS。

## Hook

Hook 允许在迁移的各个生命周期阶段执行自定义逻辑：

- **PipelineHook** — `OnPipelineStart` / `OnPipelineEnd`（全局）
- **TableHook** — `OnTableStart` / `OnTableEnd`（如创建 TimescaleDB hypertable）
- **BatchHook** — `OnBatchComplete`（如定时聚合）

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

## Debug 与诊断

```yaml
debug:
  enabled: true
  verbose_batch: true    # 记录每批次的耗时和行数
  log_memory: true       # 记录每批次的内存使用

pprof:
  enabled: true
  dir: "./.databridge/pprof"
  interval: "5m"         # 采集间隔
  profiles:
    - "heap"
    - "goroutine"
    - "allocs"
  cpu_duration: "30s"
```

使用以下命令分析采集到的 profile：

```bash
go tool pprof -http=:8080 .databridge/pprof/heap_20260101_120000.prof
```

## 运行测试

```bash
# 单元测试
make test

# 集成测试（需要 Docker，会自动启动 MySQL 和 PostgreSQL 容器）
make test-integration
```

## License

MIT
