# 实时用户行为漏斗分析

基于阿里天池 UserBehavior 数据集，使用 Kafka、Spark Structured Streaming、HDFS、Hive 和 MySQL，构建一个简单的用户行为实时与离线分析项目。

## ▪ 1. 项目简介

项目将用户的 `pv`、`cart`、`buy` 三种行为用于实时和离线统计。

实时部分使用 Python 模拟用户行为数据持续产生，经 Kafka 传输后，由 Spark Structured Streaming 进行窗口统计和转化率计算，最终将结果写入 MySQL。

离线部分将历史 UserBehavior 数据存入 HDFS，通过 Hive 建表、分区并使用 Hive SQL 进行分析。

主要指标：

- PV → Cart
- Cart → Buy
- PV → Buy

实时链路：

`Python → Kafka → Spark Structured Streaming → MySQL`

离线链路：

`HDFS → Hive → Hive SQL`

## ▪ 2. 项目架构

项目运行在 Windows + WSL2 Ubuntu + Docker 的单机环境中。

<img width="1278" height="461" alt="42ec378786181b35f7b635f19871b352" src="https://github.com/user-attachments/assets/546e5d81-8f77-4d6b-9a46-c86a3c884f23" />

### 实时计算

Python Producer 读取 UserBehavior 数据并发送到 Kafka。

Spark Structured Streaming 从 Kafka 读取数据，完成 JSON 解析、数据清洗、Event Time、Watermark、5 分钟窗口聚合和转化率计算，再通过 `foreachBatch` 写入 MySQL。

### 离线计算

历史 UserBehavior 数据存储在 HDFS 中，Hive 建立原始表和按日期分区的用户行为表，再通过 Hive SQL 完成 PV、UV、行为统计和转化率分析。

### 技术栈与环境

| 组件 | 版本 | 用途 | 运行方式 |
| --- | --- | --- | --- |
| Python | 3.x | Kafka 数据生产 | WSL2 Ubuntu |
| Kafka | 7.5.0 | 实时数据传输 | Docker |
| Spark | 3.5.1 | 实时数据处理 | Docker |
| Hadoop | 3.4.3 | 历史数据存储 | Docker |
| Hive | 4.0.1 | 离线数据管理与 SQL 分析 | Docker |
| MySQL | 8.0 | 保存实时计算结果 | Docker |

项目采用单机部署方式，各组件分别通过 Docker Compose 或 Docker 容器运行。

根目录的 `docker-compose.yml` 用于管理 ZooKeeper、Kafka 和 MySQL；HDFS 使用 `hdfs/docker-compose.yml` 单独管理。

## ▪ 3. 实时计算

### 3.1 数据生产

使用 Python Producer 读取 UserBehavior 数据，将每条用户行为转换成 JSON 后发送到 Kafka。

数据字段：

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

读取 Kafka 后，首先解析 JSON 数据，并过滤掉无效数据，只保留 `pv`、`cart` 和 `buy` 三种行为。

项目使用行为数据中的 `timestamp` 作为 Event Time，而不是简单按照数据到达时间进行统计。

设置 5 分钟时间窗口，并使用 5 分钟 Watermark 处理一定程度的迟到数据。

流程图：

<img width="1599" height="482" alt="image" src="https://github.com/user-attachments/assets/ee9bfb12-8e1b-47b4-8c3e-88301d96423c" />

### 3.3 转化率计算

每个时间窗口分别统计：

- PV 独立用户数
- Cart 独立用户数
- Buy 独立用户数

然后计算：

- PV → Cart = Cart 用户数 / PV 用户数
- Cart → Buy = Buy 用户数 / Cart 用户数
- PV → Buy = Buy 用户数 / PV 用户数

这里统计的是各行为的独立用户数，并不是严格要求同一用户依次完成 `PV → Cart → Buy` 的顺序漏斗。

用户数使用 Spark 的 `approx_count_distinct` 进行近似去重统计。

### 3.4 MySQL

Spark 使用 `foreachBatch` 将每个微批次的计算结果写入 MySQL。

表名：

`funnel_result`

同一个时间窗口使用唯一键 `window_start + window_end`，重复计算时更新已有结果。

Structured Streaming 同时配置了 checkpoint：

`/opt/project/checkpoint`

用于保存流处理状态和进度。

## ▪ 4. 离线计算

### 4.1 HDFS

将 UserBehavior 历史数据上传到 HDFS，作为离线分析的数据来源。

HDFS 采用单机环境：

- NameNode：1 个
- DataNode：1 个
- 数据副本数：1

数据路径：

`/user/wql/userbehavior/`

### 4.2 Hive

Hive 中建立用户行为原始表 `user_behavior`，并建立按日期分区的 `user_behavior_daily` 表。

分区字段：

`event_date`

根据 `event_timestamp` 转换得到日期，并通过动态分区写入对应分区。

Hive 建表和数据处理 SQL 位于：

`hive_sql/`

包含：

- `01_create_tables.sql`：创建数据库和用户行为表
- `02_partition_insert.sql`：动态分区写入
- `03_offline_analysis.sql`：离线统计分析

### 4.3 离线分析

离线 SQL 主要完成：

- 每日 PV / UV
- 每日各行为发生次数
- 每日各行为用户数
- 每日用户行为转化率

离线统计与实时部分保持相同的用户行为口径，便于进行结果对比。

## ▪ 5. 仓库结构

```text
realtime_funnel_analysis/
├── hdfs/                                      # HDFS 配置及 Docker 部署文件
│   ├── core-site.xml                          # Hadoop 核心配置
│   ├── docker-compose.yml                     # HDFS Docker 部署配置
│   ├── hdfs-site-datanode.xml                 # DataNode 配置
│   └── hdfs-site-namenode.xml                 # NameNode 配置
│
├── hive_sql/                                  # Hive 建表及离线分析 SQL
│   ├── 01_create_tables.sql                   # 创建数据库及用户行为表
│   ├── 02_partition_insert.sql                # 动态分区写入数据
│   └── 03_offline_analysis.sql                # PV、UV、行为及转化率分析
│
├── jars/                                      # Spark 连接 Kafka 所需依赖
│   ├── commons-pool2-2.11.1.jar
│   ├── kafka-clients-3.4.1.jar
│   ├── spark-sql-kafka-0-10_2.12-3.5.1.jar
│   └── spark-token-provider-kafka-0-10_2.12-3.5.1.jar
│
├── Dockerfile                                 # 构建 Spark 3.5.1 运行环境
├── docker-compose.yml                         # Kafka、ZooKeeper、MySQL 部署配置
├── mysql                                      # MySQL 数据库及结果表初始化 SQL
├── producer.py                                # Python Kafka 数据生产程序
├── requirements.txt                           # Python 项目依赖
├── sort_user_behavior.py                      # UserBehavior 数据整理与排序
├── spark_structured_streaming.py              # Spark Structured Streaming 实时计算
└── README.md                                  # 项目说明文档
