from typing import List, Dict, Optional
from datetime import datetime

from prefect import task
import pandas as pd


def _parse_iso_datetime(date_str: Optional[str]) -> Optional[datetime]:
    if not isinstance(date_str, str):
        return None
    try:
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None


@task
def transform_launches(
    raw_launches: List[dict], launchpads: List[dict], rockets: List[dict]
) -> List[Dict]:
    """
    Filters and transforms raw launch data by enriching it with details from launchpads and rockets.

    This function takes a list of raw launch dictionaries, along with lists of
    launchpad and rocket dictionaries. It then joins this information to produce
    a more structured and detailed list of launch records.
    Key transformations include:
    - Mapping launchpad and rocket IDs in the raw launch data to their respective
      names and other details.
    - Converting the 'date_utc' string from ISO format to a datetime object.

    Args:
        raw_launches (List[dict]): A list of dictionaries, where each dictionary
            represents a raw launch. Expected keys include 'id', 'name',
            'date_utc' (ISO string), 'details', 'launchpad' (ID),
            'rocket' (ID), 'success', 'upcoming'.
        launchpads (List[dict]): A list of dictionaries, where each dictionary
            represents a launchpad. Expected keys include 'id', 'name',
            'full_name'.
        rockets (List[dict]): A list of dictionaries, where each dictionary
            represents a rocket. Expected keys include 'id', 'name'.

    Returns:
        List[dict]: A list of transformed launch dictionaries. Each dictionary
            contains combined and processed information with the following keys:
            '_id' (original launch ID), 'name', 'date_utc' (datetime object),
            'details', 'launchpad_id', 'launchpad_name', 'launchpad_fullname',
            'rocket_id', 'rocket_name', 'success', 'upcoming'.
    """
    """
    Transforms raw launch data by enriching it with details from launchpads and rockets,
    handling missing keys and data gracefully.

    This function takes a list of raw launch dictionaries, along with lists of
    launchpad and rocket dictionaries. It then joins this information to produce
    a more structured and detailed list of launch records.
    Key transformations include:
    - Mapping launchpad and rocket IDs in the raw launch data to their respective
      names and other details. If IDs are missing or not found, related fields
      will be None.
    - Converting the 'date_utc' string from ISO format to a datetime object.
      If the date string is missing or invalid, 'date_utc' will be None.

    Args:
        raw_launches (List[dict]): A list of dictionaries, where each dictionary
            represents a raw launch. Expected keys include 'id', 'name',
            'date_utc' (ISO string), 'details', 'launchpad' (ID),
            'rocket' (ID), 'success', 'upcoming'. Missing keys are handled.
        launchpads (List[dict]): A list of dictionaries, where each dictionary
            represents a launchpad. Expected keys include 'id', 'name',
            'full_name'. Missing keys are handled.
        rockets (List[dict]): A list of dictionaries, where each dictionary
            represents a rocket. Expected keys include 'id', 'name'.
            Missing keys are handled.

    Returns:
        List[dict]: A list of transformed launch dictionaries. Each dictionary
            contains combined and processed information with the following keys:
            '_id' (original launch ID), 'name', 'date_utc' (datetime object or None),
            'details', 'launchpad_id', 'launchpad_name', 'launchpad_fullname',
            'rocket_id', 'rocket_name', 'success', 'upcoming'.
            Fields will be None if the source data was missing or unmatchable.
    """
    transformed = []
    launchpad_map: Dict[str, Dict] = {
        lp["id"]: lp for lp in launchpads if isinstance(lp, dict) and "id" in lp
    }
    rocket_map: Dict[str, Dict] = {
        r["id"]: r for r in rockets if isinstance(r, dict) and "id" in r
    }

    if not raw_launches:
        return []

    for launch in raw_launches:
        if not isinstance(launch, dict):
            continue

        launchpad_id = launch.get("launchpad")
        rocket_id = launch.get("rocket")

        launchpad_data = launchpad_map.get(launchpad_id, {}) if launchpad_id else {}
        rocket_data = rocket_map.get(rocket_id, {}) if rocket_id else {}

        transformed.append(
            {
                "_id": launch.get("id"),
                "name": launch.get("name"),
                "date_utc": _parse_iso_datetime(launch.get("date_utc")),
                "details": launch.get("details"),
                "launchpad_id": launchpad_id,
                "launchpad_name": launchpad_data.get("name"),
                "launchpad_fullname": launchpad_data.get("full_name"),
                "rocket_id": rocket_id,
                "rocket_name": rocket_data.get("name"),
                "success": launch.get("success"),
                "upcoming": launch.get("upcoming"),
            }
        )
    return transformed
