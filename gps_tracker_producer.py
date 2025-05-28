from kafka import KafkaProducer
import json
import random
import time
import datetime

NCCU_LAT = 24.9886
NCCU_LON = 121.5786

driver_ids = [1001 + i for i in range(8)]

def get_region(lat, lon):
    if lat >= NCCU_LAT and lon < NCCU_LON:
        return "政大附近"
    elif lat >= NCCU_LAT and lon >= NCCU_LON:
        return "台北市中正區"
    elif lat < NCCU_LAT and lon < NCCU_LON:
        return "台北市信義區"
    else:
        return "台北市大安區"
    
# 產生模擬的 GPS 裝置位置事件
def generate_gps_tracker_event(driver_id):
    lat = round(random.uniform(24.9796, 24.9976), 8)
    lon = round(random.uniform(121.5696, 121.5876), 8)
    heading = random.randint(0, 359)
    accuracy = round(random.uniform(1.0, 5.0), 2)
    region = get_region(lat, lon)

    return {
        "event_type": "gps_tracker",
        "driver_id": driver_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "data": {
            "location": {"latitude": lat, "longitude": lon},
            "heading_deg": heading,
            "accuracy_m": accuracy,
            "region": region
        }
    }

def produce_events(bootstrap_servers='140.119.164.16:9092', topic_name='driver-locations', duration=3000000000): # 共產生30秒，每2秒發一次
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    start_time = time.time()
    event_count = 0

    try:
        while (time.time() - start_time) < duration: 
            for driver_id in driver_ids:
                event = generate_gps_tracker_event(driver_id)
                producer.send(topic_name, event)
                event_count += 1
                print(f"Produced {event_count} events. Latest: {event['event_type']} by driver {event['driver_id']}, location:{event['data']['region']}") # print(f"[GPS 追蹤] 司機 {driver_id} 在 {event['region']} 狀態已送出")
                # print(json.dumps(event, indent=2, ensure_ascii=False)) // 可以看到整包event的內容
            time.sleep(2)
    finally:
        producer.flush()
        producer.close()
        print(f"Finished. Total events: {event_count}")

if __name__ == "__main__":
    produce_events()
