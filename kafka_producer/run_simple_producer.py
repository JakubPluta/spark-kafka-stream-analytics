import time
from dataclasses import asdict
from random import random
import random
from core.config import settings
from generator.data_generator import DataGenerator

from core.logger import get_logger
from kafka_producer.producer import ProducerConfiguration, LoanEventsProducer

logger = get_logger(__name__)


def run_kafka_producer(topic: str, events_per_second: int = 10) -> None:
    """Starts a Kafka producer to generate loan application events.

    Args:
        topic (str): The Kafka topic to which events will be produced.
        events_per_second (int, optional): The rate at which events are produced. Defaults to 10.
    """
    gen = DataGenerator()
    kafka_producer = LoanEventsProducer(
        config=ProducerConfiguration(bootstrap_servers=settings.kafka_brokers)
    )

    logger.info("Starting event generation")

    try:
        while True:
            customer_id = random.randint(1, settings.num_customers)
            event = gen.generate_loan_application_event(customer_id)
            kafka_producer.produce_event(topic=topic, event=asdict(event))
            if events_per_second > 0:
                time.sleep(1 / events_per_second)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, stopping event generation")
    except Exception as e:
        logger.error(f"An error occurred during event generation: {e}")
    finally:
        try:
            kafka_producer.flush()
            kafka_producer.close()
            logger.info("Producer finished")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")


if __name__ == "__main__":
    run_kafka_producer(settings.kafka_with_redis_input_topic, events_per_second=2)
