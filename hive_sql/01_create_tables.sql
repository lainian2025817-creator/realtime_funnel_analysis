CREATE DATABASE IF NOT EXISTS userbehavior;

USE userbehavior;

-- 用户行为原始表
CREATE TABLE IF NOT EXISTS user_behavior (
    user_id STRING,
    item_id STRING,
    category_id STRING,
    behavior STRING,
    event_timestamp BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/wql/userbehavior';


-- 按日期分区的用户行为表
CREATE TABLE IF NOT EXISTS user_behavior_daily (
    user_id STRING,
    item_id STRING,
    category_id STRING,
    behavior STRING,
    event_timestamp BIGINT
)
PARTITIONED BY (
    event_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
