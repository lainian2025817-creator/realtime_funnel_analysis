USE userbehavior;
INSERT INTO TABLE funnel_rt_d
PARTITION (event_date)
SELECT
    pv_user,
    cart_user,
    buy_user,
    CASE 
        WHEN pv_user > 0 
        THEN cart_user / pv_user 
        ELSE 0 
    END AS pv_to_cart,
    CASE 
        WHEN cart_user > 0 
        THEN buy_user / cart_user 
        ELSE 0 
    END AS cart_to_buy,
    CASE 
        WHEN pv_user > 0 
        THEN buy_user / pv_user 
        ELSE 0 
    END AS pv_to_buy,
    event_date
FROM(
    SELECT
        event_date,
        COUNT(
            CASE 
                WHEN pv_flag = 1 
                THEN user_id 
            END
        ) AS pv_user,
        COUNT(
            CASE 
                WHEN pv_flag = 1 
                AND cart_flag = 1
                THEN user_id 
            END
        ) AS cart_user,
        COUNT(
            CASE 
                WHEN pv_flag = 1 
                AND cart_flag = 1
                AND buy_flag = 1
                THEN user_id 
            END
        ) AS buy_user
    FROM(
        SELECT
            user_id,
            event_date,
            MAX(
                CASE 
                    WHEN behavior='pv' THEN 1 ELSE 0 
                END
            ) AS pv_flag,
            MAX(
                CASE 
                    WHEN behavior='cart' THEN 1 ELSE 0 
                END
            ) AS cart_flag,
            MAX(
                CASE 
                    WHEN behavior='buy' THEN 1 ELSE 0 
                END
            ) AS buy_flag
        FROM user_behavior_daily
        GROUP BYuser_id,event_date
    ) t

    GROUP BY event_date

) result;
