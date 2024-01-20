import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import requests
import datetime

# Lấy thời gian hiện tại
now = datetime.datetime.now()
current_day = now.day
current_month = now.month
current_year = now.year
print(f'match_id_{current_year}_{current_month}_{current_day}')
api_key = "RGAPI-d04b277d-1c1a-4c95-b576-9dfbeae907b9"
#consumer và producer
consumer = KafkaConsumer(
    f'match_id_{current_year}_{current_month}_{current_day}',  # Tên topic
    bootstrap_servers='localhost:9092',  # Địa chỉ của Kafka broker
    enable_auto_commit=True,
    auto_commit_interval_ms=300,
    auto_offset_reset='earliest',  # Bắt đầu đọc từ đầu nếu không tìm thấy offset
    group_id=f'match_detail_{current_year}_{current_month}_{current_day}',  # ID nhóm cho consumer
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserializer cho dữ liệu JSON
)

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    match_ids = message.value
    for match_id in match_ids:
        url = f"https://sea.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            match_detail = response.json()
            producer.send(f'match_detail_{current_year}_{current_month}_{current_day}', match_detail)
            print(f"Success vs {match_id}")
        else:
            print(f"Error vs {match_id}: Status code {response.status_code}")


producer.close()
consumer.close()