import json
from dataclasses import asdict
from functools import partial
from itertools import chain, islice
from logging import getLogger
from typing import Generator

import redis
from config import settings
from data_generator import CustomerProfile, DataGenerator

logger = getLogger(__name__)
logger.setLevel("INFO")


def chunkify(iterable, size=1000):
    """
    Yields successive chunks of a specified size from an iterable.

    This generator function takes an iterable and splits it into
    chunks of a specified size, yielding each chunk as a tuple.

    Args:
        iterable: An iterable to be divided into chunks.
        size (int): The size of each chunk. Defaults to 1000.

    Yields:
        Generator[Tuple]: A generator yielding tuples, each containing
        a chunk of the original iterable.
    """

    it = iter(iterable)
    make_islice = partial(islice, it, size - 1)
    for first in it:
        yield chain((first,), make_islice())


def store_customer_data_in_redis(
    data: Generator[CustomerProfile, None, None], redis_client: redis.Redis
) -> None:
    """
    Stores a stream of customer profiles in Redis.

    Args:
        data: An iterable of customer profiles to be stored.
        redis_client: A Redis client with an open connection to the desired
            Redis database.

    Notes:
        This function uses a Redis pipeline to efficiently store the data in
        Redis. The customer profiles are stored with a key of the form
        "customer:<customer_id>" and the values are stored as a Redis hash
        with two fields: "customer_id" and "data". The "customer_id" field
        contains the customer ID as a string, and the "data" field contains the
        customer profile as a JSON string.
    """
    for idx, chunk in enumerate(chunkify(data)):
        logger.info(f"Storing chunk {idx}")
        pipeline = redis_client.pipeline()
        for profile in chunk:
            customer_id = profile.customer_id
            key = f"customer:{customer_id}"
            pipeline.hset(
                key,
                mapping={
                    "customer_id": str(customer_id),
                    "data": json.dumps(asdict(profile)),
                },
            )

        pipeline.execute()
        logger.info(f"Stored data for customer {customer_id}")


def run():
    """
    Initializes a Redis client and generates customer profiles to be stored in Redis.

    This function creates a Redis client using the host and port specified in
    the settings. It then instantiates a DataGenerator object to generate a
    specified number of customer profiles. These profiles are subsequently
    stored in the Redis database using the store_customer_data_in_redis function.
    """

    redis_client = redis.Redis(host=settings.redis_host, port=settings.redis_port)
    gen = DataGenerator()
    profiles: Generator[CustomerProfile, None, None] = gen.generate_customer_data(
        num_customers=settings.num_customers
    )
    store_customer_data_in_redis(profiles, redis_client)


if __name__ == "__main__":
    run()
