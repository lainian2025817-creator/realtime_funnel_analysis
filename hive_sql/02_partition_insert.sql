USE userbehavior;

-- 开启动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 根据事件时间生成日期分区
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
