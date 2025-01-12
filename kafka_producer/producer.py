import json
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from random import random
from typing import Any, Callable, TypeAlias
import random
from core.config import settings
from generator.data_generator import DataGenerator

import six
import sys

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves


from kafka import KafkaProducer
from core.logger import get_logger

SerializerFunc: TypeAlias = Callable[[Any], bytes]

logger = get_logger(__name__)


def default_json_serializer(x) -> bytes:
    """Serializes the given object into a JSON bytes object.

    Args:
        x: The object to be serialized.

    Returns:
        A bytes object containing the JSON representation of the given object.
    """
    return json.dumps(x, sort_keys=True, default=str).encode("utf-8")


def default_key_serializer(x) -> bytes:
    """Serializes the given object into a JSON bytes object.

    Args:
        x: The object to be serialized.

    Returns:
        A bytes object containing the JSON representation of the given object.
    """
    return str(x).encode("utf-8")


class EventsProducer(ABC):
    @abstractmethod
    def produce_event(self, topic: str, event: dict, **kwargs: Any) -> None:
        raise NotImplementedError


@dataclass
class ProducerConfiguration:
    bootstrap_servers: str
    value_serializer: SerializerFunc = default_json_serializer
    key_serializer: SerializerFunc = default_key_serializer

    def as_dict(self):
        """Converts the ProducerConfiguration dataclass instance into a dictionary."""
        return asdict(self)


class LoanEventsProducer(EventsProducer):
    def __init__(
        self, config: ProducerConfiguration, **other_kafka_producer_kwargs: Any
    ):
        """Initializes a Kafka producer with the given configuration parameters.

        Args:
            config (ProducerConfiguration): The Kafka producer configuration parameters.
            other_kafka_producer_kwargs (dict, optional): Additional keyword arguments to be passed to the KafkaProducer constructor.

        Raises:
            Exception: If an error occurs while initializing the Kafka producer.
        """
        self.config = config
        self.other_params = other_kafka_producer_kwargs or {}
        try:
            logger.info(f"Initializing Kafka producer with params: {self.other_params}")
            self.producer = self._initialize_producer(
                **{**self.config.as_dict(), **self.other_params}
            )
        except (TypeError, AttributeError) as e:
            logger.error(f"Failed to initialize Kafka producer configuration: {e}")
            raise e
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise e

    @staticmethod
    def _initialize_producer(**kwargs) -> KafkaProducer:
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

    def produce_event(self, topic: str, event: dict, **kwargs: Any) -> None:
        """Produces a Kafka event with the given topic and event data.

        Args:
            topic (str): The topic to which the event will be produced.
            event (dict): The event data to be produced. It must be a dictionary

        """
        try:
            logger.info(f"Producing event: event_id: {event['event_id']}")
            self.producer.send(topic, key=str(event["event_id"]), value=event)
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            raise e

    def flush(self):
        """Flushes the Kafka producer."""
        self.producer.flush()

    def close(self):
        self.producer.close()
