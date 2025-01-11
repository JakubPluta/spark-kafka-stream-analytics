import dataclasses
import datetime
import os
import time
import uuid
from copy import copy
import random

from confluent_kafka import SerializingProducer
import simplejson as json
from config import Settings


@dataclasses.dataclass
class Coordinates:
    latitude: float
    longitude: float


LONDON_COORDS = Coordinates(51.5074, -0.1278)
BIRMINGHAM_COORDS = Coordinates(52.4862, -1.8904)

LATITUDE_INCREMENT = (BIRMINGHAM_COORDS.latitude - LONDON_COORDS.latitude) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDS.longitude - LONDON_COORDS.longitude) / 100


# global variables
start_time = datetime.datetime.now(tz=datetime.timezone.utc)
start_location = copy(LONDON_COORDS)


def _simulate_vehicle_move():
    global start_location

    start_location.latitude += LATITUDE_INCREMENT
    start_location.longitude += LONGITUDE_INCREMENT

    start_location.latitude += random.uniform(-0.0005, 0.0005)
    start_location.longitude += random.uniform(-0.0005, 0.0005)
    return start_location


def _get_next_time():
    global start_time
    start_time += datetime.timedelta(seconds=random.randint(20, 60))
    return start_time


def generate_vehicle_data(device_id: str):
    location = _simulate_vehicle_move()
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "location": (location.latitude, location.longitude),
        "timestamp": _get_next_time().isoformat(),
        "speed": random.uniform(10, 40),
        "direction": "NE",
        "model": "Civic",
        "make": "Honda",
        "year": 2023,
        "fuel": "hybrid",
    }


def generate_gps_data(device_id, timestamp, vehicle_type="private"):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0, 42),
        "direction": "NE",
        "vehicle_type": vehicle_type,
    }


def generate_traffic_camera_data(device_id, timestamp, camera_id):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "camera_id": camera_id,
        "snapshot": "Base64EncodedString",
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "temperature": random.uniform(5, 26),
        "humidity": random.uniform(0, 100),
        "pressure": random.uniform(0, 100),
        "wind_speed": random.uniform(0, 100),
        "wind_direction": random.uniform(0, 360),
        "weather_condition": random.choice(["sunny", "cloudy", "rainy", "snowy"]),
        "air_quality_index": random.uniform(0, 100),
    }


def generate_emergency_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "severity": random.choice(["low", "medium", "high"]),
        "type": random.choice(["fire", "traffic", "accident", "police", "other"]),
        "status": random.choice(["active", "resolved"]),
        "description": "Description of the emergency",
    }


def json_serializer(x):
    if isinstance(x, uuid.UUID):
        return str(x)
    raise TypeError("Type {} not serializable".format(type(x)))


def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def produce(p: SerializingProducer, topic: str, data: dict):
    p.produce(
        topic,
        key=str(data["id"]),
        value=json.dumps(data, default=json_serializer).encode("utf-8"),
        on_delivery=delivery_report,
    )
    p.flush()


def run_simulation(p: SerializingProducer, device_id: str):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = generate_traffic_camera_data(
            device_id, vehicle_data["timestamp"], "Xiaomi Camera 1"
        )
        weather_data = generate_weather_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"]
        )
        emergency_data = generate_emergency_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"]
        )

        lat, lon = vehicle_data["location"]
        if lat >= BIRMINGHAM_COORDS.latitude and lon <= BIRMINGHAM_COORDS.longitude:
            print("Vehicle reached Birmingham. Breaking loop")
            break

        produce(p, Settings.VEHICLE_TOPIC, vehicle_data)
        produce(p, Settings.GPS_TOPIC, gps_data)
        produce(p, Settings.TRAFFIC_TOPIC, traffic_camera_data)
        produce(p, Settings.WEATHER_TOPIC, weather_data)
        produce(p, Settings.EMERGENCY_TOPIC, emergency_data)

        time.sleep(1)


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": Settings.KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print("Kafka Error: {}".format(err)),
    }

    producer = SerializingProducer(producer_config)
    try:
        run_simulation(producer, "Vehicle-1")
    except KeyboardInterrupt:
        print("Aborted by user")
    except Exception as e:
        print("Unexpected error: {}".format(e))
