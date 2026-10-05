import csv

input_file = "/mnt/d/UserBehavior/UserBehavior.csv/UserBehavior.csv"
output_file = "/mnt/d/UserBehavior/UserBehavior_sorted_1m.csv"

data = []

with open(input_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)

    for i, row in enumerate(reader):
        if i >= 1_000_000:
            break

        if len(row) != 5:
            continue

        user_id, item_id, category_id, behavior, timestamp = row

        data.append([
            user_id,
            item_id,
            category_id,
            behavior,
            int(timestamp)
        ])

print(f"读取完成：{len(data)} 条")

data.sort(key=lambda x: x[4])

with open(output_file, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)

    for row in data:
        writer.writerow(row)

print("排序完成")
print(f"输出文件：{output_file}")
