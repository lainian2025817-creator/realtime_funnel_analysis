from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    from_unixtime,
    col,
    window,
    approx_count_distinct,
    when,
    first
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

spark = SparkSession.builder \
    .appName("UserBehaviorFunnelAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("behavior", StringType(), True),
    StructField("timestamp", LongType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user_behavior") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr(
    "CAST(value AS STRING) AS json"
)

result = json_df.select(
    from_json(col("json"), schema).alias("data")
).select(
    "data.user_id",
    "data.item_id",
    "data.category_id",
    "data.behavior",
    "data.timestamp"
)

clean_df = result.filter(
    col("user_id").isNotNull()
    & col("behavior").isin("pv", "cart", "buy")
)

event_df = clean_df.withColumn(
    "event_time",
    from_unixtime(col("timestamp")).cast("timestamp")
)

window_df = (
    event_df
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes")
    )
    .agg(
        approx_count_distinct(
            when(
                col("behavior") == "pv",
                col("user_id")
            )
        ).alias("pv_user"),

        approx_count_distinct(
            when(
                col("behavior") == "cart",
                col("user_id")
            )
        ).alias("cart_user"),

        approx_count_distinct(
            when(
                col("behavior") == "buy",
                col("user_id")
            )
        ).alias("buy_user")
    )
)

result_df = window_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("pv_user"),
    col("cart_user"),
    col("buy_user"),

    when(
        col("pv_user") > 0,
        col("cart_user") / col("pv_user")
    ).otherwise(0).alias("pv_to_cart"),

    when(
        col("cart_user") > 0,
        col("buy_user") / col("cart_user")
    ).otherwise(0).alias("cart_to_buy"),

    when(
        col("pv_user") > 0,
        col("buy_user") / col("pv_user")
    ).otherwise(0).alias("pv_to_buy")
)

def write_to_mysql(batch_df, batch_id):

    import mysql.connector

    rows = batch_df.collect()

    if not rows:
        return

    print()
    print("========== 实时转化率 ==========")

    for row in rows:

        pv_to_cart = float(row.pv_to_cart)
        cart_to_buy = float(row.cart_to_buy)
        pv_to_buy = float(row.pv_to_buy)

        print(
            f"窗口：{row.window_start} ~ {row.window_end}\n"
            f"PV用户：{row.pv_user}\n"
            f"Cart用户：{row.cart_user}\n"
            f"Buy用户：{row.buy_user}\n"
            f"PV → Cart：{pv_to_cart:.2%}\n"
            f"Cart → Buy：{cart_to_buy:.2%}\n"
            f"PV → Buy：{pv_to_buy:.2%}"
        )

    print("================================")

    conn = mysql.connector.connect(
        host="mysql",
        port=3306,
        user="root",
        password="123456",
        database="realtime"
    )

    cursor = conn.cursor()

    sql = """
        INSERT INTO funnel_result (
            window_start,
            window_end,
            pv_user,
            cart_user,
            buy_user,
            pv_to_cart,
            cart_to_buy,
            pv_to_buy
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pv_user = VALUES(pv_user),
            cart_user = VALUES(cart_user),
            buy_user = VALUES(buy_user),
            pv_to_cart = VALUES(pv_to_cart),
            cart_to_buy = VALUES(cart_to_buy),
            pv_to_buy = VALUES(pv_to_buy)
    """

    for row in rows:

        cursor.execute(
            sql,
            (
                row.window_start,
                row.window_end,
                row.pv_user,
                row.cart_user,
                row.buy_user,
                row.pv_to_cart,
                row.cart_to_buy,
                row.pv_to_buy
            )
        )

    conn.commit()

    cursor.close()
    conn.close()

query = (
    result_df.writeStream
    .outputMode("update")
    .foreachBatch(write_to_mysql)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
