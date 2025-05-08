from typing import List

from prefect import task
from pymongo import MongoClient, ReplaceOne


@task
def store_launches(launches: List[dict]) -> int:
    client: MongoClient = MongoClient("mongodb://spacex:spacex@mongo:27017/")
    db = client["spacex_data"]
    launches_collection = db["launches"]
    operations = [
        ReplaceOne({"_id": launch["_id"]}, launch, upsert=True) for launch in launches
    ]
    result = launches_collection.bulk_write(operations)
    return result.bulk_api_result["nUpserted"]
