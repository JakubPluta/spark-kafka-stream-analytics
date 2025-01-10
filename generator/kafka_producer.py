import json
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from logging import getLogger
from random import random
from typing import Any, Callable, TypeAlias

from config import settings
from data_generator import DataGenerator
from kafka import KafkaProducer

logger = getLogger(__name__)

SerializerFunc: TypeAlias = Callable[[Any], bytes]


def default_json_serializer(x) -> bytes:
    return json.dumps(x, default=str).encode("utf-8")


class EventsProducer(ABC):
    @abstractmethod
    def produce_event(self, event):
        raise NotImplementedError


@dataclass
class ProducerConfiguration:
    bootstrap_servers: str
    value_serializer: SerializerFunc = default_json_serializer

    def asdict(self):
        """Converts the ProducerConfiguration dataclass instance into a dictionary."""
        return asdict(self)


class LoanEventsProducer(EventsProducer):
    def __init__(self, config: ProducerConfiguration, **other_kafka_producer_kwargs):
        self.config = config
        self.other_params = other_kafka_producer_kwargs or {}
        try:
            params = {**self.config.asdict(), **self.other_params}
            logger.info(f"Initializing Kafka producer with params: {params}")
            self.producer = self._initialize_producer(**params)
        except (TypeError, AttributeError) as e:
            logger.error(f"Failed to initialize Kafka producer configuration: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise e

    def _initialize_producer(self, **kwargs) -> KafkaProducer:
        """
        Initializes a KafkaProducer instance with given configuration parameters.

        Uses the provided keyword arguments to configure the KafkaProducer,
        allowing customization of settings such as bootstrap servers, topic,
        and value serializer.

        Returns:
            KafkaProducer: An instance of KafkaProducer configured with the
            specified parameters.
        """
        return KafkaProducer(**kwargs)

    def produce_event(self, topic, event):
        try:
            self.producer.send(topic, event)
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            raise e

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()


def run_kafka_producer(topic, events_per_second=10):
    gen = DataGenerator()
    kafka_producer = LoanEventsProducer(
        config=ProducerConfiguration(bootstrap_servers="localhost:9092")
    )
    topic = "loan_application_events"

    try:
        while True:
            customer_id = random.randint(1, settings.num_customers)
            event = gen.generate_loan_application_event(customer_id)
            kafka_producer.produce_event(topic=topic, event=asdict(event))
            if events_per_second:
                time.sleep(1 / events_per_second)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, stopping event generation")
    finally:
        kafka_producer.flush()
        kafka_producer.close()
        logger.info("Producer finished")
