import random
import string
from datetime import datetime, timedelta
import mysql.connector

# 生成随机字符串
def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

# 生成随机时间
def random_time(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

# 数据条数
num_records = 10

# 定义时间范围
start_time = datetime(2025, 1, 1)
end_time = datetime(2025, 5, 6)

# ods_user_behavior 表数据
user_behavior_data = []
action_types = ['搜索', '浏览', '购买', '关注']
for i in range(num_records):
    behavior_id = f"BH{i + 1:03d}"
    user_id = f"U{i + 1:03d}"
    product_id = f"P{i + 1:03d}"
    action_type = random.choice(action_types)
    action_time = random_time(start_time, end_time)
    search_keywords = random_string(10) if action_type == '搜索' else None
    user_behavior_data.append((behavior_id, user_id, product_id, action_type, action_time, search_keywords))

# ods_refund 表数据
refund_data = []
refund_statuses = ['处理中', '成功', '关闭']
refund_reasons = ['商品质量问题', '不喜欢', '尺寸不合适']
for i in range(num_records):
    refund_id = f"RF{i + 1:03d}"
    order_id = f"O{i + 1:03d}"
    refund_time = random_time(start_time, end_time)
    refund_amount = round(random.uniform(10, 1000), 2)
    refund_status = random.choice(refund_statuses)
    refund_reason = random.choice(refund_reasons)
    refund_data.append((refund_id, order_id, refund_time, refund_amount, refund_status, refund_reason))

# ods_logistics 表数据
logistics_data = []
for i in range(num_records):
    logistics_id = f"LG{i + 1:03d}"
    order_id = f"O{i + 1:03d}"
    receive_time = random_time(start_time, end_time)
    update_time = random_time(receive_time, end_time)
    delivery_time = random_time(update_time, end_time)
    sign_time = random_time(delivery_time, end_time)
    logistics_data.append((logistics_id, order_id, receive_time, update_time, delivery_time, sign_time))

# ods_order 表数据
order_data = []
order_statuses = ['已完成', '待发货', '已发货']
for i in range(num_records):
    order_id = f"O{i + 1:03d}"
    user_id = f"U{i + 1:03d}"
    payment_time = random_time(start_time, end_time)
    promised_ship_time = random_time(payment_time, end_time)
    actual_ship_time = random_time(payment_time, end_time) if random.random() > 0.2 else None
    order_amount = round(random.uniform(10, 1000), 2)
    status = random.choice(order_statuses)
    order_data.append((order_id, user_id, payment_time, promised_ship_time, actual_ship_time, order_amount, status))

# 连接到 MySQL 数据库
try:
    mydb = mysql.connector.connect(
        host="localhost",
        user="your_username",
        password="your_password",
        database="your_database"
    )
    mycursor = mydb.cursor()

    # 插入 ods_user_behavior 表数据
    user_behavior_sql = "INSERT INTO ods_user_behavior (behavior_id, user_id, product_id, action_type, action_time, search_keywords) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.executemany(user_behavior_sql, user_behavior_data)
    mydb.commit()
    print(mycursor.rowcount, "条记录插入到 ods_user_behavior 表。")

    # 插入 ods_refund 表数据
    refund_sql = "INSERT INTO ods_refund (refund_id, order_id, refund_time, refund_amount, refund_status, refund_reason) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.executemany(refund_sql, refund_data)
    mydb.commit()
    print(mycursor.rowcount, "条记录插入到 ods_refund 表。")

    # 插入 ods_logistics 表数据
    logistics_sql = "INSERT INTO ods_logistics (logistics_id, order_id, receive_time, update_time, delivery_time, sign_time) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.executemany(logistics_sql, logistics_data)
    mydb.commit()
    print(mycursor.rowcount, "条记录插入到 ods_logistics 表。")

    # 插入 ods_order 表数据
    order_sql = "INSERT INTO ods_order (order_id, user_id, payment_time, promised_ship_time, actual_ship_time, order_amount, status) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(order_sql, order_data)
    mydb.commit()
    print(mycursor.rowcount, "条记录插入到 ods_order 表。")

except mysql.connector.Error as err:
    print(f"数据库错误: {err}")
finally:
    if mydb.is_connected():
        mycursor.close()
        mydb.close()
        print("数据库连接已关闭。")