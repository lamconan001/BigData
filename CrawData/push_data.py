import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer
import datetime
import json

# Lấy thời gian hiện tại
now = datetime.datetime.now()
current_day = now.day
current_month = now.month
current_year = now.year
client = pymongo.MongoClient("mongodb+srv://cbnprinze:cbnprince@cbnprince.qijvvlq.mongodb.net/?retryWrites=t")

# Chọn hoặc tạo database
mydb = client["ten_database"]

# Chọn hoặc tạo collection
mycol = mydb["ten_collection"]

# Dữ liệu cần đẩy lên

consumer = KafkaConsumer(
    f'match_detail_{current_year}_{current_month}_{current_day}',  # Tên topic
    bootstrap_servers='localhost:9092',  # Địa chỉ của Kafka broker
    enable_auto_commit=True,
    auto_commit_interval_ms=300,
    auto_offset_reset='earliest',  # Bắt đầu đọc từ đầu nếu không tìm thấy offset
    group_id=f'insert_to_db_{current_year}_{current_month}_{current_day}',  # ID nhóm cho consumer
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserializer cho dữ liệu JSON
)

for message in consumer:
    match_detail = message.value
    mycol.insert_one(match_detail)
    print("Dữ liệu đã được đẩy lên MongoDB!")
# Đẩy dữ liệu lên collection


