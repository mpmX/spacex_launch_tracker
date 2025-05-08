import os
from typing import List
from prefect import flow
from tasks.fetch import get_launches, get_launchpads, get_rockets
from tasks.transform import transform_launches
from tasks.store import store_launches
from tasks.notify import notify_webhook

INTERVAL_MINUTES = int(os.environ.get("DATA_SYNC_INTERVAL_MINUTES", 60))


@flow(log_prints=True)
def update_spacex_launches():
    raw_launches: List[dict] = get_launches()
    launchpads: List[dict] = get_launchpads()
    rockets: List[dict] = get_rockets()
    launches: List[dict] = transform_launches(
        raw_launches=raw_launches, launchpads=launchpads, rockets=rockets
    )
    new_launches: int = store_launches(launches)
    if new_launches > 0:
        notify_webhook(f"{new_launches} new launches!")


if __name__ == "__main__":
    update_spacex_launches()
    update_spacex_launches.serve(
        name="SpaceX Launch Data Update", interval=INTERVAL_MINUTES * 60
    )
