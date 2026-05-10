[English](README.md) | 简体中文

# databridge

在不同数据库之间轻松迁移数据。

## 特性

- **插件化架构** — 新增数据库只需实现接口，无需修改核心代码
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

即将支持：MongoDB、Redis、Kafka、InfluxDB、ClickHouse。

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

1. 实现 `source.Source` 和/或 `sink.Sink` 接口
2. 实现 Schema 映射（`SourceTypeMapper` / `TargetTypeMapper`）
3. 在 `init()` 中注册：`source.Register("名称", factory)` / `sink.Register("名称", factory)`
4. 在 `cmd/databridge/main.go` 中 import 插件包

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
