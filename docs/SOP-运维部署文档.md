# 冷热数据自动归档系统 - 运维部署 SOP

## 文档信息

| 项目 | 内容 |
|------|------|
| 文档名称 | 冷热数据自动归档系统运维部署 SOP |
| 版本 | v1.0.0 |
| 适用版本 | datalake-archive-scheduler v1.0.0 |
| 编写日期 | 2024-01-15 |
| 负责人 | 数据平台组 |

---

## 1. 系统概述

### 1.1 系统简介

冷热数据自动归档系统是面向金融级高并发场景的底层数据归档平台，负责将主业务库（PolarDB）中的冷数据（三年前）自动抽取、清洗、脱敏后归档至对象存储（OSS），并通过 Hive Metastore 注册元数据，最终导入 StarRocks 历史宽表，实现冷热数据分离与成本优化。

### 1.2 系统特性

- **多协程并发抽取**：支持分片并行处理，充分利用系统资源
- **流式数据处理**：基于 Channel 的三阶段 Pipeline，有效控制内存使用
- **事务补偿机制**：Saga 模式的补偿逻辑，确保数据最终一致性
- **数据脱敏**：支持敏感字段自动脱敏，满足金融合规要求
- **内存保护**：内存上限控制，防止 OOM
- **DDD 架构**：领域驱动设计，各层完全解耦，便于扩展维护

### 1.3 核心技术栈

| 层次 | 技术 |
|------|------|
| 语言 | Go 1.21 |
| Web 框架 | Gin v1.9.1 |
| 日志 | Zap v1.26.0 |
| 配置 | Viper v1.17.0 |
| 主数据库 | PolarDB (MySQL 协议) |
| 对象存储 | 阿里云 OSS |
| 元数据 | Hive Metastore |
| 数仓 | StarRocks |

---

## 2. 架构设计

### 2.1 DDD 分层架构

```
┌─────────────────────────────────────────────────┐
│           Interfaces (接口层)                    │
│  Gin Handler / Router / Middleware              │
└─────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│          Application (应用层)                    │
│  ArchiveAppService / StreamProcessor            │
│  CompensationService / DTO                      │
└─────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│            Domain (领域层)                       │
│  Entities / Value Objects / Repo Interfaces     │
│  Domain Services (Cleaning / Masking / Serial)  │
└─────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│       Infrastructure (基础设施层)                │
│  PolarDB Repo / OSS Repo                        │
│  Hive Metastore Repo / StarRocks Repo           │
│  Config / Logger                                │
└─────────────────────────────────────────────────┘
```

### 2.2 数据流转架构

```
┌──────────┐
│  PolarDB  │
│ (主业务库) │
└─────┬─────┘
      │ 1. 定时扫描冷数据
      ▼
┌─────────────────────────────┐
│  Stream Processor           │
│  ┌─────────┐ ┌─────────┐   │
│  │ 抽取协程 │ │ 清洗协程 │   │
│  └────┬────┘ └────┬────┘   │
│       └──────►─────┘        │
│              │              │
│         ┌────▼────┐         │
│         │ 脱敏协程 │         │
│         └────┬────┘         │
└──────────────┼──────────────┘
               │ 2. 序列化打包
               ▼
┌──────────┐
│   OSS     │
│(对象存储)  │
└─────┬─────┘
      │ 3. 注册分区元数据
      ▼
┌──────────────────┐
│ Hive Metastore   │
│  (元数据中心)     │
└─────┬────────────┘
      │ 4. 导入历史宽表
      ▼
┌──────────────────┐
│   StarRocks      │
│ unicorn_pro_history│
└──────────────────┘
```

### 2.3 并发分片模型

```
ArchiveJob
├── Shard-0 (ID: 1-10000)
├── Shard-1 (ID: 10001-20000)
├── Shard-2 (ID: 20001-30000)
│    ...
└── Shard-N (ID: XXXXX-YYYYY)

并发控制: Semaphore (Concurrency = 5)
每 Shard 内部: Extract → Clean → Mask → OSS Upload (Pipeline)
```

---

## 3. 部署指南

### 3.1 环境要求

| 资源 | 最低配置 | 推荐配置 |
|------|----------|----------|
| CPU | 4 核 | 8 核 |
| 内存 | 8 GB | 16 GB |
| 磁盘 | 100 GB SSD | 500 GB SSD |
| 操作系统 | CentOS 7.9+ / Ubuntu 20.04+ | CentOS 7.9 / Ubuntu 22.04 |
| Go 版本 | 1.21+ | 1.21+ |

### 3.2 依赖组件

| 组件 | 版本要求 | 说明 |
|------|----------|------|
| PolarDB | MySQL 5.7/8.0 兼容 | 主业务数据源 |
| 阿里云 OSS | - | 冷数据存储 |
| Hive Metastore | 3.1+ | 元数据管理 |
| StarRocks | 2.5+ | 历史宽表查询 |

### 3.3 安装部署

#### 3.3.1 源码编译

```bash
# 1. 克隆代码
git clone <repository-url>
cd 01-datalake-archive-scheduler

# 2. 下载依赖
go mod download

# 3. 编译
go build -o bin/archive-scheduler ./cmd/archive-scheduler

# 4. 验证
./bin/archive-scheduler --help
```

#### 3.3.2 Docker 部署

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o archive-scheduler ./cmd/archive-scheduler

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/archive-scheduler .
COPY config.yaml .
EXPOSE 8080
ENTRYPOINT ["./archive-scheduler"]
```

```bash
# 构建镜像
docker build -t datalake-archive-scheduler:v1.0.0 .

# 运行容器
docker run -d \
  --name archive-scheduler \
  -p 8080:8080 \
  -v /path/to/config.yaml:/app/config.yaml \
  -e DB_PASSWORD=your_password \
  -e OSS_ACCESS_KEY=your_key \
  -e OSS_SECRET_KEY=your_secret \
  datalake-archive-scheduler:v1.0.0
```

#### 3.3.3 Kubernetes 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: archive-scheduler
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: archive-scheduler
  template:
    metadata:
      labels:
        app: archive-scheduler
    spec:
      containers:
      - name: archive-scheduler
        image: datalake-archive-scheduler:v1.0.0
        ports:
        - containerPort: 8080
        env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: archive-secrets
              key: db-password
        - name: OSS_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: archive-secrets
              key: oss-access-key
        - name: OSS_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: archive-secrets
              key: oss-secret-key
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "4"
            memory: "8Gi"
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: archive-scheduler-svc
  namespace: data-platform
spec:
  selector:
    app: archive-scheduler
  ports:
  - port: 8080
    targetPort: 8080
```

### 3.4 配置说明

配置文件 `config.yaml` 详细说明：

#### 3.4.1 服务配置 (server)

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| port | int | 8080 | 服务监听端口 |
| mode | string | release | Gin 运行模式：debug/release/test |
| read_timeout | int | 60 | 读取超时（秒） |
| write_timeout | int | 60 | 写入超时（秒） |

#### 3.4.2 数据库配置 (database)

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| driver | string | mysql | 数据库驱动 |
| host | string | localhost | 数据库地址 |
| port | int | 3306 | 数据库端口 |
| user | string | - | 用户名 |
| password | string | - | 密码 |
| dbname | string | - | 数据库名 |
| max_open_conn | int | 100 | 最大连接数 |
| max_idle_conn | int | 20 | 最大空闲连接数 |
| max_lifetime | int | 3600 | 连接最大生命周期（秒） |

#### 3.4.3 OSS 配置 (oss)

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| endpoint | string | - | OSS 端点 |
| access_key | string | - | Access Key |
| secret_key | string | - | Secret Key |
| bucket | string | - | Bucket 名称 |
| path_prefix | string | archive | 路径前缀 |
| region | string | - | 区域 |
| use_ssl | bool | true | 是否使用 SSL |

#### 3.4.4 归档配置 (archive)

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| cold_years | int | 3 | 冷数据年限 |
| shard_count | int | 10 | 分片数量 |
| concurrency | int | 5 | 并发数 |
| batch_size | int | 1000 | 批处理大小 |
| max_retry_count | int | 3 | 最大重试次数 |
| memory_limit_mb | int | 512 | 内存限制（MB） |
| masking_salt | string | - | 脱敏盐值 |
| table_name | string | order_detail | 源表名 |
| target_table | string | unicorn_pro_history | 目标表名 |
| cron_expr | string | 0 2 3 * * ? | Cron 表达式 |

---

## 4. 操作手册

### 4.1 启动服务

```bash
# 方式一：直接启动
./bin/archive-scheduler

# 方式二：指定配置文件启动
./bin/archive-scheduler /path/to/config.yaml

# 方式三：Systemd 服务管理
systemctl start archive-scheduler
systemctl enable archive-scheduler
```

### 4.2 API 接口

所有 API 路径前缀：`/api/v1`

#### 4.2.1 创建归档任务

- **接口**：`POST /archive/jobs`
- **描述**：创建一个新的归档任务
- **请求体**：

```json
{
  "table_name": "order_detail",
  "cold_date": "2021-01-01",
  "shard_count": 10,
  "concurrency": 5
}
```

- **响应**：

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "job_name": "archive-order_detail-20210101",
    "status": "PENDING",
    "created_at": "2024-01-15T10:00:00Z"
  }
}
```

#### 4.2.2 启动归档任务

- **接口**：`POST /archive/jobs/:jobId/start`
- **描述**：启动指定的归档任务
- **响应**：

```json
{
  "code": 200,
  "message": "Job started successfully",
  "data": {
    "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  }
}
```

#### 4.2.3 查询任务状态

- **接口**：`GET /archive/jobs/:jobId`
- **描述**：查询指定任务的状态
- **响应**：

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "job_name": "archive-order_detail-20210101",
    "table_name": "order_detail",
    "status": "RUNNING",
    "total_records": 1000000,
    "archived_count": 350000,
    "failed_count": 0,
    "progress": 35.0,
    "shard_count": 10,
    "concurrency": 5,
    "start_time": "2024-01-15T10:05:00Z",
    "end_time": "",
    "error_message": ""
  }
}
```

#### 4.2.4 查询分片状态

- **接口**：`GET /archive/jobs/:jobId/shards`
- **描述**：查询任务的所有分片状态
- **响应**：

```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "shard_index": 0,
      "status": "COMPLETED",
      "record_count": 100000,
      "oss_path": "2021/01/01/shard_0000.jsonl",
      "retry_count": 0,
      "error_message": ""
    },
    {
      "shard_index": 1,
      "status": "RUNNING",
      "record_count": 0,
      "oss_path": "",
      "retry_count": 0,
      "error_message": ""
    }
  ]
}
```

#### 4.2.5 任务列表

- **接口**：`GET /archive/jobs?limit=20&offset=0`
- **描述**：获取归档任务列表

#### 4.2.6 暂停任务

- **接口**：`POST /archive/jobs/:jobId/pause`
- **描述**：暂停正在运行的归档任务

#### 4.2.7 恢复任务

- **接口**：`POST /archive/jobs/:jobId/resume`
- **描述**：恢复已暂停的归档任务

#### 4.2.8 补偿任务

- **接口**：`POST /archive/jobs/:jobId/compensate`
- **描述**：对失败的任务启动补偿机制
- **响应**：

```json
{
  "code": 200,
  "message": "Compensation task started",
  "data": {
    "compensation_id": "comp-xyz-123",
    "status": "RUNNING",
    "retry_shard_count": 2
  }
}
```

#### 4.2.9 系统统计

- **接口**：`GET /stats`
- **描述**：获取系统整体统计信息

#### 4.2.10 健康检查

- **接口**：`GET /health`
- **描述**：服务健康检查

### 4.3 常用操作示例

#### 4.3.1 创建并启动归档任务

```bash
# 创建任务
JOB_ID=$(curl -s -X POST http://localhost:8080/api/v1/archive/jobs \
  -H "Content-Type: application/json" \
  -d '{"table_name": "order_detail", "cold_date": "2021-01-01"}' | jq -r '.data.job_id')

echo "Job ID: $JOB_ID"

# 启动任务
curl -X POST http://localhost:8080/api/v1/archive/jobs/$JOB_ID/start
```

#### 4.3.2 监控任务进度

```bash
#!/bin/bash

JOB_ID=$1

while true; do
  STATUS=$(curl -s http://localhost:8080/api/v1/archive/jobs/$JOB_ID | jq -r '.data.status')
  PROGRESS=$(curl -s http://localhost:8080/api/v1/archive/jobs/$JOB_ID | jq -r '.data.progress')

  echo "Status: $STATUS, Progress: ${PROGRESS}%"

  if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ]; then
    break
  fi

  sleep 5
done
```

#### 4.3.3 失败任务补偿

```bash
JOB_ID="failed-job-id"

# 启动补偿
curl -X POST http://localhost:8080/api/v1/archive/jobs/$JOB_ID/compensate

# 查看补偿后分片状态
curl http://localhost:8080/api/v1/archive/jobs/$JOB_ID/shards | jq '.data[] | select(.status=="FAILED")'
```

---

## 5. 故障排查

### 5.1 常见问题

#### 5.1.1 服务启动失败

**现象**：服务启动后立即退出

**排查步骤**：

1. 检查配置文件是否存在且格式正确
   ```bash
   ls -la config.yaml
   cat config.yaml | head -n 20
   ```

2. 查看日志输出
   ```bash
   journalctl -u archive-scheduler -n 100 --no-pager
   ```

3. 检查端口是否被占用
   ```bash
   netstat -tlnp | grep 8080
   ```

#### 5.1.2 任务执行失败

**现象**：任务状态变为 FAILED

**排查步骤**：

1. 查看任务错误信息
   ```bash
   curl http://localhost:8080/api/v1/archive/jobs/<job_id> | jq '.data.error_message'
   ```

2. 查看失败分片详情
   ```bash
   curl http://localhost:8080/api/v1/archive/jobs/<job_id>/shards | jq '.data[] | select(.status=="FAILED")'
   ```

3. 检查 PolarDB 连接
   ```bash
   mysql -h <host> -u <user> -p -e "SELECT 1"
   ```

4. 检查 OSS 连通性
   ```bash
   ping oss-cn-hangzhou.aliyuncs.com
   ```

#### 5.1.3 内存占用过高

**现象**：服务内存占用持续上升

**排查步骤**：

1. 检查当前内存配置
   ```bash
   grep memory_limit config.yaml
   ```

2. 降低并发数和分片数
   ```yaml
   archive:
     concurrency: 3
     batch_size: 500
     memory_limit_mb: 256
   ```

3. 监控内存使用
   ```bash
   watch -n 1 'ps aux | grep archive-scheduler'
   ```

#### 5.1.4 OSS 上传失败

**现象**：分片上传 OSS 时失败

**排查步骤**：

1. 检查 OSS 配置是否正确
2. 检查网络连通性
3. 检查 Bucket 权限
4. 查看错误信息中的具体原因

**解决方案**：

- 增大超时时间
- 启用重试机制
- 检查网络带宽

#### 5.1.5 StarRocks 导入失败

**现象**：数据导入 StarRocks 失败

**排查步骤**：

1. 检查 StarRocks FE 状态
2. 检查目标表是否存在
3. 检查导入格式是否正确
4. 查看 StarRocks 日志

### 5.2 紧急回滚方案

当归档任务出现严重问题，需要回滚时：

#### 5.2.1 回滚步骤

1. **暂停任务**
   ```bash
   curl -X POST http://localhost:8080/api/v1/archive/jobs/<job_id>/pause
   ```

2. **评估影响范围**
   - 已完成分片数
   - 已归档记录数
   - 是否影响业务

3. **清理 OSS 数据**
   ```bash
   # 删除已上传的分片文件
   ossutil rm -r oss://bucket/archive/2021/01/01/
   ```

4. **清理 Hive 分区**
   ```sql
   ALTER TABLE unicorn_pro_history DROP IF EXISTS PARTITION (dt='2021-01-01');
   ```

5. **清理 StarRocks 数据**
   ```sql
   DELETE FROM unicorn_pro_history WHERE dt = '2021-01-01';
   ```

6. **恢复源数据**（如已删除）
   - 从备份恢复 PolarDB 数据

---

## 6. 性能调优

### 6.1 并发调优

| 场景 | 推荐配置 | 说明 |
|------|----------|------|
| 低资源环境 | shard_count=4, concurrency=2 | 节省资源 |
| 标准配置 | shard_count=10, concurrency=5 | 平衡性能 |
| 高并发环境 | shard_count=20, concurrency=10 | 最大化吞吐量 |

### 6.2 内存调优

| 内存配置 | batch_size | 说明 |
|----------|------------|------|
| 512 MB | 1000 | 保守配置 |
| 1 GB | 2000 | 标准配置 |
| 2 GB | 5000 | 高性能配置 |

### 6.3 数据库连接调优

```yaml
database:
  max_open_conn: 200   # 提高最大连接数
  max_idle_conn: 50    # 提高空闲连接数
  max_lifetime: 1800   # 缩短连接生命周期
```

### 6.4 OSS 上传优化

- 使用 OSS 内网地址，减少网络延迟
- 启用分片上传，提高大文件上传速度
- 使用 OSS 传输加速功能

---

## 7. 安全规范

### 7.1 认证鉴权

- 所有 API 接口必须经过认证
- 建议使用 JWT 或 OAuth2.0
- 定期轮换密钥

### 7.2 数据安全

- 敏感字段必须脱敏
- 传输过程使用 HTTPS
- OSS 数据启用加密
- 定期审计归档数据访问日志

### 7.3 网络安全

- 服务部署在内网
- 使用防火墙限制访问
- 数据库和 OSS 使用 VPC 内网访问

### 7.4 权限管理

| 角色 | 权限 |
|------|------|
| 管理员 | 全部权限 |
| 运维人员 | 任务管理、查看权限 |
| 开发人员 | 查看权限 |
| 只读用户 | 状态查询权限 |

---

## 8. 监控告警

### 8.1 监控指标

| 指标 | 说明 | 阈值 |
|------|------|------|
| 服务存活 | 健康检查 | < 1 = 告警 |
| 任务失败率 | 失败任务数/总任务数 | > 5% 告警 |
| 归档延迟 | 预期完成时间 - 实际完成时间 | > 24h 告警 |
| 内存使用率 | 内存使用/配置上限 | > 80% 告警 |
| API 响应时间 | P95 响应时间 | > 1s 告警 |
| OSS 上传失败率 | 失败次数/总次数 | > 1% 告警 |

### 8.2 告警配置

```yaml
# Prometheus 监控示例
groups:
- name: archive-scheduler
  rules:
  - alert: ArchiveJobFailed
    expr: rate(archive_job_failed_total[5m]) > 0
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "归档任务失败"
      description: "任务 {{ $labels.job_id }} 执行失败"

  - alert: HighMemoryUsage
    expr: go_memstats_heap_alloc_bytes > 536870912  # 512MB
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "内存使用率过高"
      description: "归档服务内存使用超过 512MB"
```

### 8.3 日志规范

| 日志级别 | 使用场景 |
|----------|----------|
| DEBUG | 详细调试信息 |
| INFO | 正常流程信息 |
| WARN | 警告信息，不影响运行 |
| ERROR | 错误信息，需要关注 |
| FATAL | 致命错误，服务无法运行 |

---

## 9. 附录

### 9.1 术语表

| 术语 | 说明 |
|------|------|
| DDD | 领域驱动设计 (Domain-Driven Design) |
| Shard | 数据分片 |
| Compensation | 事务补偿 |
| Cold Data | 冷数据（访问频率低的数据） |
| Pipeline | 流水线式处理 |
| Saga | 一种分布式事务模式 |

### 9.2 相关文档

- [Gin 官方文档](https://gin-gonic.com/)
- [阿里云 OSS 文档](https://help.aliyun.com/product/oss/)
- [StarRocks 官方文档](https://docs.starrocks.io/)
- [Apache Hive 文档](https://cwiki.apache.org/confluence/display/Hive/Home)

### 9.3 版本历史

| 版本 | 日期 | 变更内容 | 作者 |
|------|------|----------|------|
| v1.0.0 | 2024-01-15 | 初始版本 | 数据平台组 |

---

**文档结束**
