import dataclasses
import datetime
import os
import uuid
from copy import copy
import random

from confluent_kafka import SerializingProducer
import simplejson as json


@dataclasses.dataclass
class Coordinates:
    latitude: float
    longitude: float


LONDON_COORDS = Coordinates(51.5074, -0.1278)
BIRMINGHAM_COORDS = Coordinates(52.4862, -1.8904)

LATITUDE_INCREMENT = (BIRMINGHAM_COORDS.latitude - LONDON_COORDS.latitude) / 10
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDS.longitude - LONDON_COORDS.longitude) / 10

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency")

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
        'id': uuid.uuid4(),
        "device_id": device_id,
        "location": (location.latitude, location.longitude),
        "timestamp": _get_next_time().isoformat(),
        "speed": random.uniform(10, 40),
        'direction': 'NE',
        "model": "Civic",
        "make": "Honda",
        "year": 2023,
        "fuel": "hybrid",
    }


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 42),
        'direction': 'NE',
        'vehicle_type': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp):
    pass


def run_simulation(p: SerializingProducer, device_id: str):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        break


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print('Kafka Error: {}'.format(err)),
    }

    producer = SerializingProducer(producer_config)
    try:
        run_simulation(producer, 'Vehicle-1')
    except KeyboardInterrupt:
        print('Aborted by user')
    except Exception as e:
        print("Unexpected error: {}".format(e))
