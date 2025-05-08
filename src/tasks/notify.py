import os
import json
from prefect import task
from pymongo import MongoClient
import httpx

from exceptions.external_api_exception import ExternalAPIException

MONGODB_USERNAME = os.environ["MONGODB_USERNAME"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]
MONGODB_DB_NAME = os.environ["MONGODB_DB_NAME"]
MONGODB_WEBHOOKS_COLLECTION = os.environ["MONGODB_WEBHOOKS_COLLECTION"]
MONGO_URL = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@mongo:27017/"


@task(retries=1, retry_delay_seconds=5)
def notify_webhook(message: str):
    """
    Sends a message to a configured webhook URL retrieved from MongoDB.

    This function connects to a MongoDB instance using credentials and database
    details specified by environment variables (MONGODB_USERNAME, MONGODB_PASSWORD,
    MONGODB_DB_NAME, MONGODB_WEBHOOKS_COLLECTION). It attempts to find a
    webhook configuration document with `_id: 1` in the specified webhooks
    collection.

    If a document is found and contains a "url" field, an HTTP POST request
    is made to that URL with the provided message in a JSON payload
    (e.g., `{"message": "your_message_here"}`).

    The function is decorated as a Prefect task (`@task`) and will retry once
    after a 5-second delay in case of failure.

    Various HTTP and network errors during the POST request (e.g., status errors,
    timeouts, request errors, JSON decoding issues) are caught and re-raised as
    an `ExternalAPIException`, providing more context about the failure.

    If no webhook document with `_id: 1` is found, or if it lacks a "url" field,
    the function will silently do nothing (no notification will be sent and no
    error will be raised for this specific case).

    Args:
        message (str): The message string to be sent to the webhook.

    Raises:
        ExternalAPIException: If an error occurs while making the HTTP POST
                              request to the webhook URL. This can be due to
                              various reasons like network issues, server errors
                              (non-2xx status codes), request timeouts, problems
                              decoding a JSON response (if applicable), or other
                              unexpected request-related errors.
        pymongo.errors.ConnectionFailure: If connection to MongoDB fails.
        pymongo.errors.OperationFailure: If a MongoDB operation fails.
    """
    client: MongoClient = MongoClient(MONGO_URL)
    webhooks_collection = client[MONGODB_DB_NAME][MONGODB_WEBHOOKS_COLLECTION]
    webhook = webhooks_collection.find_one({"_id": 1})
    if webhook and "url" in webhook:
        try:
            httpx.post(webhook["url"], json={"message": message})
        except httpx.HTTPStatusError as e:
            raise ExternalAPIException(
                message=f"API returned an error status {e.response.status_code}",
                status_code=e.response.status_code,
                url=webhook["url"],
                response_text=e.response.text,
                original_exception=e,
            )
        except httpx.TimeoutException as e:
            raise ExternalAPIException(
                message="Request timed out", url=webhook["url"], original_exception=e
            )
        except httpx.RequestError as e:
            raise ExternalAPIException(
                message=f"A request error occurred: {e}",
                url=webhook["url"],
                original_exception=e,
            )
        except json.JSONDecodeError as e:
            raise ExternalAPIException(
                message="Failed to decode JSON response",
                url=webhook["url"],
                original_exception=e,
            )
        except Exception as e:
            raise ExternalAPIException(
                message=f"An unexpected error occurred: {e}",
                url=webhook["url"],
                original_exception=e,
            )
