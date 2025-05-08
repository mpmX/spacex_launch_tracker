from prefect import task
from pymongo import MongoClient


@task
def notify_webhook(message: str):
    pass
