import os
from typing import List

from prefect import task
from pymongo import MongoClient, ReplaceOne

MONGODB_USERNAME = os.environ["MONGODB_USERNAME"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]
MONGODB_DB_NAME = os.environ["MONGODB_DB_NAME"]
MONGODB_LAUNCHES_COLLECTION = os.environ["MONGODB_LAUNCHES_COLLECTION"]
MONGO_URL = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@mongo:27017/"


@task
def store_launches(launches: List[dict]) -> int:
    """
    Stores or updates a list of launch documents in a MongoDB collection.

    This function connects to a MongoDB instance using credentials and connection
    details specified by environment variables:
    - MONGODB_USERNAME
    - MONGODB_PASSWORD
    - MONGODB_DB_NAME (database name)
    - MONGODB_LAUNCHES_COLLECTION (collection name)

    It performs a bulk `ReplaceOne` operation with `upsert=True`. This means
    for each launch document provided:
    - If a document with the same `_id` already exists in the collection,
      it will be replaced with the new document.
    - If no document with that `_id` exists, the new document will be inserted.

    Args:
        launches: A list of dictionaries, where each dictionary represents a
            launch document. Each dictionary **must** contain an `_id` field,
            which is used as the unique identifier for the upsert operation.

    Returns:
        int: The number of documents that were upserted (i.e., newly inserted
             or updated if they already existed) into the collection. This
             corresponds to the `nUpserted` field from the bulk write result.

    Raises:
        pymongo.errors.ConnectionFailure: If connection to MongoDB fails.
        pymongo.errors.OperationFailure: If the bulk write operation fails for
                                         reasons other than connection issues.
        KeyError: If any of the required environment variables are not set.
    """
    client: MongoClient = MongoClient(MONGO_URL)
    launches_collection = client[MONGODB_DB_NAME][MONGODB_LAUNCHES_COLLECTION]
    operations = [
        ReplaceOne({"_id": launch["_id"]}, launch, upsert=True) for launch in launches
    ]
    result = launches_collection.bulk_write(operations)
    return result.bulk_api_result["nUpserted"]
