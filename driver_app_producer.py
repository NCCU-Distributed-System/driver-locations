from kafka import KafkaProducer
import json
import random
import time
import datetime
import math
import threading
"""
預設乘客位置在NCCU
司機會在台北市內生成100個，每個美0.5秒移動5公尺(約時速30~40)
"""
TAIPEI_LAT_MIN = 25.001000
TAIPEI_LAT_MAX = 25.132000
TAIPEI_LON_MIN = 121.457000
TAIPEI_LON_MAX = 121.613000

NCCU_LAT = 24.988600
NCCU_LON = 121.578600

driver_ids = [1001 + i for i in range(100)]
driver_states = {}
lock = threading.Lock()

# 初始化司機位置
def initialize_driver_states():
    for driver_id in driver_ids:
        lat = round(random.uniform(TAIPEI_LAT_MIN, TAIPEI_LAT_MAX), 6)
        lon = round(random.uniform(TAIPEI_LON_MIN, TAIPEI_LON_MAX), 6)
        driver_states[driver_id] = {"latitude": lat, "longitude": lon}
        
# 使用小範圍劃分模擬行政區資料
def get_region(lat, lon):
    if not (TAIPEI_LAT_MIN <= lat <= TAIPEI_LAT_MAX and TAIPEI_LON_MIN <= lon <= TAIPEI_LON_MAX):
        return "台北市境外"

    # 模擬行政區邊界
    if 25.120000 <= lat <= 25.132000 and 121.500000 <= lon <= 121.540000:
        return "北投區"
    elif 25.110000 <= lat < 25.120000 and 121.540000 <= lon <= 121.580000:
        return "士林區"
    elif 25.070000 <= lat < 25.110000 and 121.500000 <= lon <= 121.540000:
        return "中山區"
    elif 25.060000 <= lat < 25.070000 and 121.550000 <= lon <= 121.590000:
        return "松山區"
    elif 25.040000 <= lat < 25.060000 and 121.570000 <= lon <= 121.610000:
        return "內湖區"
    elif 25.030000 <= lat < 25.040000 and 121.500000 <= lon <= 121.550000:
        return "中正區"
    elif 25.010000 <= lat < 25.030000 and 121.520000 <= lon <= 121.560000:
        return "大安區"
    elif 25.000000 <= lat < 25.010000 and 121.560000 <= lon <= 121.600000:
        return "文山區"
    elif 25.000000 <= lat < 25.010000 and 121.500000 <= lon < 121.560000:
        return "萬華區"
    elif 25.060000 <= lat < 25.100000 and 121.580000 <= lon <= 121.613000:
        return "南港區"
    else:
        return "信義區"


# 計算兩個經緯度之間距離
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c  # km

# 模擬更新司機位置函式
def update_driver_location():
    delta = 0.000045  # 約 5 公尺
    directions = ['N', 'S', 'E', 'W']
    while True:
        time.sleep(0.5)
        with lock:
            for driver_id in driver_ids:
                direction = random.choice(directions)
                state = driver_states[driver_id].copy()

                if direction == 'N':
                    state['latitude'] += delta
                elif direction == 'S':
                    state['latitude'] -= delta
                elif direction == 'E':
                    state['longitude'] += delta
                elif direction == 'W':
                    state['longitude'] -= delta
                
                driver_states[driver_id] = state
                
# # 根據經緯度分類到特定區域，原先為["後山", "動物園捷運站", "水鋼琴社區", "木柵好樂迪"]，目前不代表真正位置，統一格式複製 @顏聖峰 的code後改的名稱
# def get_region(lat, lon):
#     if lat >= NCCU_LAT and lon < NCCU_LON:
#         return "政大附近"
#     elif lat >= NCCU_LAT and lon >= NCCU_LON:
#         return "台北市中正區"
#     elif lat < NCCU_LAT and lon < NCCU_LON:
#         return "台北市信義區"
#     else:
#         return "台北市大安區"
    
# 產生模擬的司機位置更新事件
def generate_driver_app_event(driver_id):
    state = driver_states[driver_id]
    # 隨機產生在政大方圓 1 公里的經緯度
    lat, lon = round(state['latitude'], 6), round(state['longitude'], 6)

    speed = round(random.uniform(20, 50), 2) # 假設目前車速為 20~50 km/h 之間
    distance_km = haversine(lat, lon, NCCU_LAT, NCCU_LON) # 計算目前距離政大的直線距離
    eta_min = round((distance_km / (speed / 30)), 2) # 根據車速估算到達政大所需時間（分鐘）
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
    
    initialize_driver_states() # 初始化司機# 開始位置更新執行緒
    threading.Thread(target=update_driver_location, daemon=True).start() # 開始位置更新執行緒
    start_time = time.time() # 紀錄開始時間
    event_count = 0          # 紀錄事件數量
    printed_8 = False  # 標記是否已印出 driver_id=1008 的資料
    try:
        # 在 duration 秒內持續傳送資料
        while (time.time() - start_time) < duration:
            all_events = []
            with lock:
                for driver_id in driver_ids:
                    event = generate_driver_app_event(driver_id)
                    all_events.append(event) 
                                       
                    # ✅ Print 每位司機的事件資料（精簡）
                    print(f"[{event['timestamp']}] Driver {driver_id}: {event['data']}")

                    # ✅ 特別印出第108位司機 (driver_id=1108)
                    if driver_id == 1008 and not printed_8:
                        print("\n=== DRIVER 1008 FULL EVENT DATA ===")
                        print(json.dumps(event, indent=2, ensure_ascii=False))
                        print("=== END OF DRIVER 1008 EVENT DATA ===\n")
                        printed_8 = True
                    
            if all_events:
                producer.send(topic_name, all_events)
                event_count += len(all_events)
                print(f" - Driver {event['driver_id']} in {event['data']['region']} | ETA: {event['data']['estimated_arrival_min']} min")
                # 以下舊的log
                # print(f"Produced {event_count} events. Latest: {event['event_type']} by driver {event['driver_id']}, location:{event['data']['region']} ") # print(f"[Driver App] Sent for driver {driver_id} - ETA: {event['estimated_arrival_min']} min in {event['region']}")
                # print(json.dumps(event, indent=2, ensure_ascii=False)) #// 可以看到整包event的內容
            time.sleep(0.5)
    except KeyboardInterrupt:  
        print("Event production stopped manually")
    finally:
        # 時間(duration)結束時關閉 Producer
        producer.flush()
        producer.close()
        print(f"Finished. Total events: {event_count}")

if __name__ == "__main__":
    produce_events()
