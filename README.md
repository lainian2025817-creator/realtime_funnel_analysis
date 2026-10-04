# 实时用户行为漏斗分析

基于阿里天池 UserBehavior 数据集，使用 Kafka、Spark Structured Streaming、HDFS、Hive 和 MySQL，构建一个简单的用户行为实时与离线分析项目。

## ▪ 1. 项目简介

这是一个基于阿里天池 UserBehavior 数据集做的用户行为分析项目。

项目把用户的 `pv`、`cart`、`buy` 三种行为放到一条实时计算链路中，用 Kafka 模拟用户行为数据持续产生，再由 Spark Structured Streaming 进行窗口统计，最后将实时结果写入 MySQL。

同时保留了一条离线分析链路，将历史数据存入 HDFS，通过 Hive 建表、分区并进行 SQL 分析。

项目主要关注三个转化指标：

- PV → Cart
- Cart → Buy
- PV → Buy

实时部分：

`Python → Kafka → Spark Structured Streaming → MySQL`

离线部分：

`HDFS → Hive → Hive SQL`

## ▪ 2. 项目架构

项目分为实时计算和离线计算两部分，整体运行在 Windows + WSL2 Ubuntu + Docker 环境中。

<img width="1278" height="461" alt="42ec378786181b35f7b635f19871b352" src="https://github.com/user-attachments/assets/713565db-6d27-4924-b795-5401406c3d4b" />

### 实时计算

Python Producer 模拟用户行为数据产生，写入 Kafka。

Spark Structured Streaming 从 Kafka 读取数据，完成数据解析、清洗、时间窗口统计和转化率计算，结果通过 `foreachBatch` 写入 MySQL。

### 离线计算

历史数据存储在 HDFS 中，Hive 建立分区表进行管理，再通过 Hive SQL 完成 PV、UV、行为统计和转化率分析。

### 技术栈与环境

| 组件 | 版本 | 用途 | 运行方式 |
| --- | --- | --- | --- |
| Python | 3.x | 模拟用户行为数据写入 Kafka | WSL2 Ubuntu |
| Kafka | 7.5.0 | 实时数据传输 | Docker 单机 |
| Spark | 3.5.1 | 实时数据处理 | Docker 单机 |
| Hadoop | 3.4.3 | 历史数据存储 | Docker 单机 |
| Hive | 4.0.1 | 离线数据管理与 SQL 分析 | Docker 单机 |
| MySQL | 8.0 | 保存实时计算结果 | Docker 单机 |
| Docker | 29.6.2 | 运行各组件 | Windows + WSL2 |

### 部署方式

本项目采用**单机环境模拟大数据处理流程**，Windows + WSL2 Ubuntu + Docker 的方式搭建单机大数据环境。

通过 Docker 容器运行Kafka、MySQL、HDFS、Hive 和 Spark 等组件，并使用 Docker Compose 统一管理。

实时计算链路：

`Python → Kafka → Spark Structured Streaming → MySQL`

离线计算链路：

`HDFS → Hive → Hive SQL`

## ▪ 3. 实时计算

实时部分主要模拟用户行为数据不断产生的场景。

### 3.1 数据生产

使用 Python Producer 读取 UserBehavior 数据，将每条用户行为转换成 JSON 后发送到 Kafka。

数据包含以下字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `user_id` | STRING | 用户 ID |
| `item_id` | STRING | 商品 ID |
| `category_id` | STRING | 商品类别 ID |
| `behavior` | STRING | 用户行为 |
| `timestamp` | BIGINT | 行为时间戳 |

Kafka Topic：

`user_behavior`

Topic 设置为 3 个 Partition，Producer 使用 `user_id` 作为 Key，使同一用户的数据尽量进入同一个 Partition。

### 3.2 Spark Structured Streaming

Spark Structured Streaming 从 Kafka 持续读取数据。

处理流程：

`Kafka → JSON 解析 → 数据清洗 → Event Time + Watermark → 5分钟窗口聚合 → 转化率计算 → MySQL`

实时流程图：

<img width="1599" height="482" alt="image" src="https://github.com/user-attachments/assets/ee9bfb12-8e1b-47b4-8c3e-88301d96423c" />

读取 Kafka 后，首先将 JSON 数据解析成结构化字段，并过滤掉无效数据，只保留 `pv`、`cart` 和 `buy` 三种行为。

### 3.3 时间窗口统计

项目使用用户行为数据中的 `timestamp` 作为 Event Time，而不是简单按照数据到达时间进行统计。

设置 5 分钟时间窗口，并使用 5 分钟 Watermark 处理一定程度的迟到数据。

每个窗口分别统计：

- PV 独立用户数
- Cart 独立用户数
- Buy 独立用户数

在此基础上计算：

`PV → Cart = Cart 用户数 / PV 用户数`

`Cart → Buy = Buy 用户数 / Cart 用户数`

`PV → Buy = Buy 用户数 / PV 用户数`

这里统计的是各行为的独立用户数，并不是严格按照同一用户依次完成 `PV → Cart → Buy` 的顺序漏斗。

### 3.4 结果写入 MySQL

Spark 使用 `foreachBatch` 对每个微批次的数据进行处理，并将计算结果写入 MySQL。

MySQL 中保存窗口时间、各行为用户数以及三个转化率指标。

表名：

`funnel_result`

同一个时间窗口使用唯一键进行更新，避免重复写入相同窗口的结果。

## ▪ 4. 离线计算

离线部分主要用于对历史用户行为数据进行统计分析。

### 4.1 HDFS 数据存储

将 UserBehavior 历史数据上传到 HDFS，作为离线分析的数据来源。

HDFS 采用单机环境搭建：

- NameNode：1 个
- DataNode：1 个
- 数据副本数：1

数据存储路径：

`/user/wql/userbehavior/`

### 4.2 Hive 数据表

在 Hive 中建立用户行为原始表 `user_behavior`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `user_id` | STRING | 用户 ID |
| `item_id` | STRING | 商品 ID |
| `category_id` | STRING | 商品类别 ID |
| `behavior` | STRING | 用户行为 |
| `event_timestamp` | BIGINT | 行为时间戳 |

在原始表基础上建立按日期分区的 `user_behavior_daily` 表：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `user_id` | STRING | 用户 ID |
| `item_id` | STRING | 商品 ID |
| `category_id` | STRING | 商品类别 ID |
| `behavior` | STRING | 用户行为 |
| `event_timestamp` | BIGINT | 行为时间戳 |
| `event_date` | STRING | 行为日期，作为分区字段 |

`event_date` 根据 `event_timestamp` 转换得到，用于按天组织和查询数据。

### 4.3 分区数据

使用 Hive 动态分区，将原始数据按照 `event_date` 写入对应分区。

```sql
INSERT INTO TABLE user_behavior_daily
PARTITION (event_date)
SELECT
    user_id,
    item_id,
    category_id,
    behavior,
    event_timestamp,
    from_unixtime(event_timestamp, 'yyyy-MM-dd') AS event_date
FROM user_behavior;
```
## 5. 仓库结构

```
realtime_funnel_analysis/
├── jars/                           # Spark Kafka 相关依赖
├── hdfs/                           # HDFS 配置文件
├── Dockerfile                      # Spark Docker 镜像配置
├── docker-compose.yml              # Docker 服务配置
├── producer.py                     # Kafka 数据生产程序
├── spark_structured_streaming.py   # Spark Structured Streaming 实时计算
├── sort_user_behavior.py           # UserBehavior 数据处理脚本
└── README.md                       # 项目说明
```

其中：

- "producer.py"：读取 UserBehavior 数据并发送到 Kafka。
- "spark_structured_streaming.py"：使用 Spark Structured Streaming 完成实时数据处理和转化率计算。
- "docker-compose.yml"：统一管理 Kafka、MySQL、HDFS、Hive 等 Docker 服务。
- "hdfs/"：保存 HDFS 相关配置。
- "jars/"：保存 Spark 连接 Kafka 所需的依赖包。

