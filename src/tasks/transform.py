from typing import List
from datetime import datetime

from prefect import task
import pandas as pd


@task
def transform_launches(
    raw_launches: List[dict], launchpads: List[dict], rockets: List[dict]
) -> List[dict]:
    transformed = []
    launchpads: pd.DataFrame = pd.DataFrame(launchpads)
    launchpads.index = launchpads.id
    rockets: pd.DataFrame = pd.DataFrame(rockets)
    rockets.index = rockets.id
    for launch in raw_launches:
        launchpad = launchpads.loc[launch["launchpad"]]
        rocket = rockets.loc[launch["rocket"]]
        transformed.append(
            {
                "_id": launch["id"],
                "name": launch["name"],
                "date_utc": datetime.fromisoformat(launch["date_utc"]),
                "details": launch["details"],
                "launchpad_id": launch["launchpad"],
                "launchpad_name": launchpad["name"],
                "launchpad_fullname": launchpad["full_name"],
                "rocket_id": launch["rocket"],
                "rocket_name": rocket["name"],
                "success": launch["success"],
                "upcoming": launch["upcoming"],
            }
        )
    return transformed
