from kafka import KafkaConsumer
from kafka import KafkaProducer
import requests

import datetime

# Lấy thời gian hiện tại
now = datetime.datetime.now()
current_day = now.day
current_month = now.month
current_year = now.year
print(f"riot_id_{current_year}_{current_month}_{current_day}")
# Tạo Kafka consumer
api_key = "RGAPI-0a1c9646-d707-42de-b3e9-eb433516ec24"
consumer = KafkaConsumer(
    f"riot_id_{current_year}_{current_month}_{current_day}",  # Tên topic
    bootstrap_servers='localhost:9092',  # Địa chỉ của Kafka broker
    enable_auto_commit=True,
    auto_commit_interval_ms=300,
    auto_offset_reset='earliest',  # Bắt đầu đọc từ đầu nếu không tìm thấy offset
    group_id=f'puuid_{current_year}_{current_month}_{current_day}'  # ID nhóm cho consumer
)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
# Xử lý mỗi message từ consumer
for message in consumer:
    # Decode message từ binary thành chuỗi
    riot_id = message.value.decode('utf-8')
    riot_name, riot_tagline = riot_id.rsplit("#", 1)
    # In thông tin ra màn hình hoặc xử lý theo yêu cầu
    print(f"Riot Name: {riot_name}, Riot Tagline: {riot_tagline}")

    url = f"https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{riot_name}/{riot_tagline}?api_key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        # Parse the JSON response
        summoner_data = response.json()

        # Extract the desired fields
        puuid = summoner_data.get('puuid', '')
        producer.send(f'puuid_{current_year}_{current_month}_{current_day}', f'{puuid}'.encode())
        producer.flush()
        print(f"Success vs {riot_name}, {riot_tagline}, {puuid}")

    else:
        print(f"Error vs {riot_name}, {riot_tagline}: Status code {response.status_code}")

# Đừng quên close consumer khi không còn sử dụng
consumer.close()
producer.close()