USE userbehavior;


-- 1. 每日 PV / UV
SELECT
    event_date,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv
FROM user_behavior_daily
GROUP BY event_date
ORDER BY event_date;


-- 2. 每日各行为发生次数
SELECT
    event_date,
    behavior,
    COUNT(*) AS behavior_count
FROM user_behavior_daily
GROUP BY event_date, behavior
ORDER BY event_date, behavior;


-- 3. 每日各行为对应的用户数
SELECT
    event_date,
    behavior,
    COUNT(DISTINCT user_id) AS user_count
FROM user_behavior_daily
GROUP BY event_date, behavior
ORDER BY event_date, behavior;


-- 4. 每日用户行为转化率
SELECT
    event_date,
    pv_user,
    cart_user,
    buy_user,
    ROUND(cart_user / pv_user, 4) AS pv_to_cart,
    ROUND(buy_user / cart_user, 4) AS cart_to_buy,
    ROUND(buy_user / pv_user, 4) AS pv_to_buy
FROM (
    SELECT
        event_date,

        COUNT(DISTINCT CASE
            WHEN behavior = 'pv' THEN user_id
        END) AS pv_user,

        COUNT(DISTINCT CASE
            WHEN behavior = 'cart' THEN user_id
        END) AS cart_user,

        COUNT(DISTINCT CASE
            WHEN behavior = 'buy' THEN user_id
        END) AS buy_user

    FROM user_behavior_daily
    GROUP BY event_date
) t
ORDER BY event_date;
