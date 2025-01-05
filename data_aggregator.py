from kafka import KafkaProducer
from configs import kafka_config, topic_prefix
import json
import time
import random


producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

building_sensors = f"{topic_prefix}_building_sensors"

id_sensor = random.randint(1000, 9999)

try:
    while True:
        data = {
            "sensor_id": id_sensor,
            "timestamp": int(time.time()),
            "temperature": random.uniform(25, 45),
            "humidity": random.uniform(15, 85),
        }
        producer.send(building_sensors, key=str(id_sensor), value=json.dumps(data))

        print(f"Sent data: {data}")

        time.sleep(1)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.flush()
    producer.close()
