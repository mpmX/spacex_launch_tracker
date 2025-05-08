import os
from datetime import timedelta
from typing import List, Dict

from prefect import task
from prefect.tasks import task_input_hash

from spacex.client import SpaceXAPIClient


CACHE_EXPIRY_MINUTES = int(os.environ.get("CACHE_EXPIRY_MINUTES", 5))


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=CACHE_EXPIRY_MINUTES),
)
def get_launches() -> List[Dict]:
    """Fetches all launches from the SpaceX API.

    This function is a Prefect task that retrieves all launch data from the
    SpaceX API using the `SpaceXAPIClient`. Results are cached based on
    input hashes for a duration specified by `CACHE_EXPIRY_MINUTES`.

    Returns:
        List[Dict]: A list of dictionaries, where each dictionary contains
                    details of a single SpaceX launch.

    Raises:
        ExternalAPIException: If an error occurs during communication with
                              the SpaceX API (e.g., network issues, API errors).
    """
    with SpaceXAPIClient() as client:
        launches = client.get_launches()
    return launches


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=CACHE_EXPIRY_MINUTES),
)
def get_launchpads() -> List[Dict]:
    """Fetches all launchpads from the SpaceX API.

    This function is a Prefect task that retrieves all launchpad data from the
    SpaceX API using the `SpaceXAPIClient`. Results are cached based on
    input hashes for a duration specified by `CACHE_EXPIRY_MINUTES`.

    Returns:
        List[Dict]: A list of dictionaries, where each dictionary contains
                    details of a single SpaceX launchpad.

    Raises:
        ExternalAPIException: If an error occurs during communication with
                              the SpaceX API (e.g., network issues, API errors).
    """
    with SpaceXAPIClient() as client:
        launchpads = client.get_launchpads()
    return launchpads


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=CACHE_EXPIRY_MINUTES),
)
def get_rockets() -> List[Dict]:
    """Fetches all rockets from the SpaceX API.

    This function is a Prefect task that retrieves all rocket data from the
    SpaceX API using the `SpaceXAPIClient`. Results are cached based on
    input hashes for a duration specified by `CACHE_EXPIRY_MINUTES`.

    Returns:
        List[Dict]: A list of dictionaries, where each dictionary contains
                    details of a single SpaceX rocket.

    Raises:
        ExternalAPIException: If an error occurs during communication with
                              the SpaceX API (e.g., network issues, API errors).
    """
    with SpaceXAPIClient() as client:
        rockets = client.get_rockets()
    return rockets
