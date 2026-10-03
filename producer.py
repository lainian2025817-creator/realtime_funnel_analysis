from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3
)

topic = "user_behavior"

MAX_RECORDS = 137_774
RECORDS_PER_SECOND = 5000
send_interval = 1 / RECORDS_PER_SECOND

total_count = 0
current_count = 0

start_time = time.time()
last_print_time = start_time

file_path = "/mnt/d/UserBehavior/UserBehavior_2017-12-02.csv"

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        if total_count >= MAX_RECORDS:
            break

        line = line.strip()

        if not line:
            continue

        parts = line.split(",")

        if len(parts) != 5:
            continue

        user_id, item_id, category_id, behavior, timestamp = parts

        data = {
            "user_id": user_id,
            "item_id": item_id,
            "category_id": category_id,
            "behavior": behavior,
            "timestamp": int(timestamp)
        }

        producer.send(topic, key=user_id, value=data)

        total_count += 1
        current_count += 1

        current_time = time.time()

        if current_time - last_print_time >= 1:
            print(
                f"当前生产速度：{current_count} 条/s，"
                f"总生产量：{total_count} 条"
            )
            current_count = 0
            last_print_time = current_time

        time.sleep(send_interval)

producer.flush()
producer.close()

end_time = time.time()
total_time = end_time - start_time
average_speed = total_count / total_time if total_time > 0 else 0

print()
print("================================")
print("UserBehavior 数据生产完成")
print(f"总生产量：{total_count} 条")
print(f"总耗时：{total_time:.2f} 秒")
print(f"平均生产速度：{average_speed:.2f} 条/s")
print("================================")
