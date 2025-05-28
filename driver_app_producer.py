from kafka import KafkaProducer
import json
import random
import time
import datetime
import math

NCCU_LAT = 24.9886
NCCU_LON = 121.5786

driver_ids = [1001 + i for i in range(8)]

# 計算兩個經緯度之間距離，由AI計算
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c  # km

# 根據經緯度分類到特定區域，原先為["後山", "動物園捷運站", "水鋼琴社區", "木柵好樂迪"]，目前不代表真正位置，統一格式複製 @顏聖峰 的code後改的名稱
def get_region(lat, lon):
    if lat >= NCCU_LAT and lon < NCCU_LON:
        return "政大附近"
    elif lat >= NCCU_LAT and lon >= NCCU_LON:
        return "台北市中正區"
    elif lat < NCCU_LAT and lon < NCCU_LON:
        return "台北市信義區"
    else:
        return "台北市大安區"
    
# 產生模擬的司機位置更新事件
def generate_driver_app_event(driver_id):
    # 隨機產生在政大方圓 1 公里的經緯度
    lat = round(random.uniform(24.9796, 24.9976), 6)
    lon = round(random.uniform(121.5696, 121.5876), 6)
    
    speed = round(random.uniform(20, 50), 2) # 假設目前車速為 20~50 km/h 之間
    distance_km = haversine(lat, lon, NCCU_LAT, NCCU_LON) # 計算目前距離政大的直線距離
    eta_min = round((distance_km / (speed / 60)), 2) # 根據車速估算到達政大所需時間（分鐘）
    region = get_region(lat, lon) # 決定此點落在哪一個自訂區域

    return {
        "event_type": "driver_app", # 統一格式改為"event_type"
        "driver_id": driver_id, # user_id 改 driver_id
        "timestamp": datetime.datetime.now().isoformat(),
        "data": {
            "location": {"latitude": lat, "longitude": lon},
            "speed_kmph": speed,
            "distance_to_nccu_km": round(distance_km, 3),
            "estimated_arrival_min": eta_min,
            "region": region
        }
    }
    
# 發送資料到Kafka
def produce_events(bootstrap_servers='140.119.164.16:9092', topic_name='driver-locations', duration=3000000000):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    start_time = time.time() # 紀錄開始時間
    event_count = 0          # 紀錄事件數量

    try:
        # 在 duration 秒內持續傳送資料
        while (time.time() - start_time) < duration:
            for driver_id in driver_ids:
                event = generate_driver_app_event(driver_id)
                producer.send(topic_name, event)
                event_count += 1
                print(f"Produced {event_count} events. Latest: {event['event_type']} by driver {event['driver_id']}, location:{event['data']['region']} ") # print(f"[Driver App] Sent for driver {driver_id} - ETA: {event['estimated_arrival_min']} min in {event['region']}")
                #print(json.dumps(event, indent=2, ensure_ascii=False)) #// 可以看到整包event的內容
            time.sleep(2)
    finally:
        # 時間(duration)結束時關閉 Producer
        producer.flush()
        producer.close()
        print(f"Finished. Total events: {event_count}")

if __name__ == "__main__":
    produce_events()
