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
print(f'puuid_{current_year}_{current_month}_{current_day}')
api_key = "RGAPI-0a1c9646-d707-42de-b3e9-eb433516ec24"
# consumer và producer
consumer = KafkaConsumer(
    f'puuid_{current_year}_{current_month}_{current_day}',  # Tên topic
    bootstrap_servers='localhost:9092',  # Địa chỉ của Kafka broker
    enable_auto_commit=True,
    auto_commit_interval_ms=300,
    auto_offset_reset='earliest',  # Bắt đầu đọc từ đầu nếu không tìm thấy offset
    group_id=f'match_id_{current_year}_{current_month}_{current_day}'  # ID nhóm cho consumer
)

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    puuid = message.value.decode('utf-8')
    url = f"https://sea.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count=5&api_key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        match_data = response.json()
        producer.send(f'match_id_{current_year}_{current_month}_{current_day}', match_data)
        print(f"Success vs {puuid}")
    else:
        print(f"Error vs {puuid}: Status code {response.status_code}")


producer.close()
consumer.close()
