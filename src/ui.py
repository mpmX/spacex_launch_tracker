import os
from typing import Optional, List
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
from datetime import datetime, date, time
import pandas as pd
import plotly.express as px

from stats import (
    calculate_rocket_success,
    calculate_group_by_counts,
    calculate_time_bucket_counts,
)

MONGODB_USERNAME = os.environ["MONGODB_USERNAME"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]
MONGODB_DB_NAME = os.environ["MONGODB_DB_NAME"]
MONGODB_LAUNCHES_COLLECTION = os.environ["MONGODB_LAUNCHES_COLLECTION"]
MONGODB_WEBHOOKS_COLLECTION = os.environ["MONGODB_WEBHOOKS_COLLECTION"]
MONGO_URL = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@mongo:27017/"


@st.cache_resource
def get_mongo_client() -> Optional[MongoClient]:
    """Establishes and returns a connection to the MongoDB server.

    The connection is cached using Streamlit's `st.cache_resource` to avoid
    re-establishing it on every script rerun. It attempts to verify the
    connection by running the 'ismaster' command.

    Returns:
        Optional[MongoClient]: A MongoClient instance if the connection is successful,
                               None otherwise. Errors are displayed via `st.error`.
    """
    try:
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=3000)
        client.admin.command("ismaster")  # Verify connection
        return client
    except ServerSelectionTimeoutError:
        st.error(
            f"Failed to connect to MongoDB at {MONGO_URL}. Is it running and accessible?"
        )
        return None


def load_initial_filter_values(
    client: MongoClient,
) -> tuple[List[str], List[str], tuple[date, date]]:
    """Loads distinct values for rocket names, launch sites, and date ranges from MongoDB.

    These values are used to populate the filter options in the Streamlit sidebar.
    It queries the database for unique `rocket_name`, `launchpad_name`, and the
    minimum and maximum `date_utc`.

    Args:
        client (MongoClient): An active MongoClient instance.

    Returns:
        tuple[List[str], List[str], tuple[date, date]]: A tuple containing:
            - A sorted list of unique rocket names.
            - A sorted list of unique launch site names.
            - A tuple with the minimum and maximum launch dates (min_date, max_date).
            Returns empty lists and today's date for min/max if the client is None
            or if an error occurs.
    """
    if client is None:
        return (
            [],
            [],
            (date.today(), date.today()),
        )  # rocket_names, launch_sites, (min_date, max_date)

    collection = client[MONGODB_DB_NAME][MONGODB_LAUNCHES_COLLECTION]

    default_min_date = date.today()
    default_max_date = date.today()

    try:
        # 1. Rocket Names
        rocket_names_cursor = collection.distinct(
            "rocket_name",
            {
                "rocket_name": {
                    "$exists": True,
                    "$ne": None,
                    "$type": "string",
                    "$nin": [""],
                }
            },
        )
        rocket_names = sorted(
            [
                r_name
                for r_name in rocket_names_cursor
                if isinstance(r_name, str) and r_name.strip()
            ]
        )

        # 2. Launch Sites
        launch_sites_cursor = collection.distinct(
            "launchpad_name",
            {
                "launchpad_name": {
                    "$exists": True,
                    "$ne": None,
                    "$type": "string",
                    "$nin": [""],
                }
            },
        )
        launch_sites = sorted(
            [
                l_site
                for l_site in launch_sites_cursor
                if isinstance(l_site, str) and l_site.strip()
            ]
        )

        # 3. Date Range
        min_date_val = default_min_date
        max_date_val = default_max_date

        min_date_doc = collection.find_one(
            {"date_utc": {"$exists": True, "$type": "date"}},
            sort=[("date_utc", 1)],
            projection={"date_utc": 1, "_id": 0},
        )
        if (
            min_date_doc
            and "date_utc" in min_date_doc
            and isinstance(min_date_doc["date_utc"], datetime)
        ):
            min_date_val = min_date_doc["date_utc"].date()
            max_date_val = min_date_val

        max_date_doc = collection.find_one(
            {"date_utc": {"$exists": True, "$type": "date"}},
            sort=[("date_utc", -1)],
            projection={"date_utc": 1, "_id": 0},
        )
        if (
            max_date_doc
            and "date_utc" in max_date_doc
            and isinstance(max_date_doc["date_utc"], datetime)
        ):
            max_date_val = max_date_doc["date_utc"].date()

        if min_date_val > max_date_val:
            min_date_val, max_date_val = max_date_val, min_date_val

        return rocket_names, launch_sites, (min_date_val, max_date_val)

    except OperationFailure as ofe:
        st.error(f"MongoDB operation error while loading filter options: {ofe}.")
        return [], [], (default_min_date, default_max_date)
    except Exception as e:
        st.error(f"Unexpected error loading filter options from MongoDB: {e}")
        return [], [], (default_min_date, default_max_date)


def build_query(
    start_date_val: date,
    end_date_val: date,
    selected_rockets: Optional[List[str]],
    selected_status: Optional[str],
    selected_sites: Optional[List[str]],
) -> dict:
    """Constructs a MongoDB query dictionary based on the provided filter criteria.

    Args:
        start_date_val (date): The start date for the date range filter.
        end_date_val (date): The end date for the date range filter.
        selected_rockets (Optional[List[str]]): A list of selected rocket names.
                                                If None or empty, no rocket filter is applied.
        selected_status (Optional[str]): The selected launch status ("Successful", "Failed",
                                         "Upcoming/TBD", or "All").
        selected_sites (Optional[List[str]]): A list of selected launch site names.
                                              If None or empty, no site filter is applied.

    Returns:
        dict: A MongoDB query dictionary.
    """
    mongo_query = {}
    # Date range filter
    if isinstance(start_date_val, date) and isinstance(end_date_val, date):
        start_datetime = datetime.combine(start_date_val, time.min)
        end_datetime = datetime.combine(end_date_val, time.max)
        mongo_query["date_utc"] = {"$gte": start_datetime, "$lte": end_datetime}
    # Rocket name filter
    if selected_rockets:
        mongo_query["rocket_name"] = {"$in": selected_rockets}
    # Success status filter
    if selected_status == "Upcoming/TBD":
        mongo_query["success"] = None
    elif selected_status == "Successful":
        mongo_query["success"] = True
    elif selected_status == "Failed":
        mongo_query["success"] = False
    # Launchpad filter
    if selected_sites:
        mongo_query["launchpad_name"] = {"$in": selected_sites}
    return mongo_query


def fetch_filtered_launches(client: MongoClient, query: dict) -> pd.DataFrame:
    """Fetches launch data from MongoDB based on the provided query.

    Args:
        client (MongoClient): An active MongoClient instance.
        query (dict): The MongoDB query dictionary to filter launches.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the filtered launch data.
                      Returns an empty DataFrame if the client is None,
                      no results are found, or an error occurs. Essential columns
                      (like '_id', 'name', 'date_utc', etc.) are ensured to exist
                      in the DataFrame, filled with None if missing in some documents.
                      The DataFrame index is set to 'date_utc'.
    """
    if client is None:
        return pd.DataFrame()

    collection = client[MONGODB_DB_NAME][MONGODB_LAUNCHES_COLLECTION]
    try:
        results = list(collection.find(query))
        if not results:
            return pd.DataFrame()
        df = pd.DataFrame(results)
        # Ensure essential columns exist even if some docs are missing them
        cols_to_check = [
            "_id",
            "name",
            "date_utc",
            "details",
            "launchpad_name",
            "rocket_name",
            "success",
            "upcoming",
        ]
        for col in cols_to_check:
            if col not in df.columns:
                df[col] = None
        df.index = df["date_utc"]
        return df
    except Exception as e:
        st.error(f"Error fetching filtered data from MongoDB: {e}")
        return pd.DataFrame()


def build_sidebar_filters(
    client: MongoClient,
) -> tuple[date, date, List[str], str, List[str]]:
    """Creates and manages Streamlit sidebar widgets for filtering launch data.

    It loads initial filter values (rocket names, sites, date range) from MongoDB
    and then displays multiselect, selectbox, and date input widgets in the sidebar.

    Args:
        client (MongoClient): An active MongoClient instance, used to load initial values.

    Returns:
        tuple[date, date, List[str], str, List[str]]: A tuple containing the current
        values selected by the user in the sidebar filters:
            - start_date_val (date): Selected start date.
            - end_date_val (date): Selected end date.
            - selected_rockets (List[str]): List of selected rocket names.
            - selected_status (str): Selected launch outcome status.
            - selected_sites (List[str]): List of selected launch sites.
    """
    (
        all_rocket_names,
        all_launch_sites,
        (min_date_db, max_date_db),
    ) = load_initial_filter_values(client)
    st.sidebar.header("Filter Launches")

    # 1. Date Range Filter
    st.sidebar.subheader("📅 Date Range")
    if not isinstance(min_date_db, date):
        min_date_db = date.today()
    if not isinstance(max_date_db, date):
        max_date_db = date.today()
    if min_date_db > max_date_db:
        min_date_db, max_date_db = max_date_db, min_date_db
    start_date_val = st.sidebar.date_input(
        "Start date", min_date_db, min_value=min_date_db, max_value=max_date_db
    )
    end_date_val = st.sidebar.date_input(
        "End date", max_date_db, min_value=start_date_val, max_value=max_date_db
    )
    # 2. Rocket Name Filter
    st.sidebar.subheader("🚀 Rocket Name")
    selected_rockets = st.sidebar.multiselect(
        "Select rocket(s)", options=all_rocket_names, default=[]
    )
    # 3. Launch Success/Failure Filter
    st.sidebar.subheader("✅ Launch Outcome")
    selected_status = st.sidebar.selectbox(
        "Select outcome",
        options=["All", "Successful", "Failed", "Upcoming/TBD"],
        index=0,
    )
    # 4. Launch Site Filter
    st.sidebar.subheader("🌍 Launch Site")
    selected_sites = st.sidebar.multiselect(
        "Select launch site(s)", options=all_launch_sites, default=[]
    )
    return (
        start_date_val,
        end_date_val,
        selected_rockets,
        selected_status,
        selected_sites,
    )


@st.dialog("Webhooks")
def webhooks_dialog(client: MongoClient):
    """Displays a Streamlit dialog for configuring a webhook URL.

    Allows users to input a webhook URL, which is then saved to or deleted from
    the MongoDB `WEBHOOK_COLLECTION_NAME`. The endpoint must accept a POST request
    with a 'message' key in the payload.

    Args:
        client (MongoClient): An active MongoClient instance to interact with the
                              webhooks collection.
    """
    webhook_collection = client[MONGODB_DB_NAME][MONGODB_LAUNCHES_COLLECTION]
    webhook = webhook_collection.find_one({"_id": 1})
    st.write(
        "Configure a webhook url to get notified when new launches occur! The endpoint must accept a POST request with a 'message' key in the payload."
    )
    url = st.text_input("Webhook URL", webhook["url"] if webhook else None)
    save_button = st.button("Save")
    if save_button:
        if url is None or not url.startswith("http://"):
            webhook_collection.delete_one({"_id": 1})
        else:
            webhook_collection.replace_one(
                {"_id": 1}, {"_id": 1, "url": url}, upsert=True
            )
        st.rerun()


def draw_metrics_horizontally(metrics: dict, unit: Optional[str] = None):
    """Displays a dictionary of metrics horizontally using Streamlit columns.

    Each key-value pair in the `metrics` dictionary is displayed as a
    Streamlit metric widget (`st.metric`) in its own column.

    Args:
        metrics (dict): A dictionary where keys are the metric labels (str)
                        and values are the metric values (numeric or str).
        unit (Optional[str], optional): A unit string to append to the metric values.
                                       Defaults to None.
    """
    columns = st.columns(len(metrics))
    for i, (k, v) in enumerate(metrics.items()):
        with columns[i]:
            st.metric(label=k, value=f"{v} {unit}" if unit else v)


def build_launches_table(df: pd.DataFrame):
    """Displays a formatted table of launch data in the Streamlit application.

    This function takes a DataFrame of launch data, selects and formats
    specific columns for presentation, and then renders it using `st.dataframe`.
    It shows the total count of launches found based on the filters.

    The 'date_utc' column is formatted to 'YYYY-MM-DD HH:MM UTC', and the
    'success' column is mapped to user-friendly strings with emojis.

    Args:
        df (pd.DataFrame): A DataFrame containing the launch data to be displayed.
                           Expected to have columns like 'name', 'date_utc',
                           'rocket_name', 'launchpad_name', 'success', 'details'.
    """
    st.write(f"Found {len(df)} launches matching your criteria.")
    display_columns = [
        "name",
        "date_utc",
        "rocket_name",
        "launchpad_name",
        "success",
        "details",
    ]
    display_columns = [col for col in display_columns if col in df.columns]
    df_display = df[display_columns].reset_index(drop=True)

    if "date_utc" in df_display.columns:
        df_display["date_utc"] = pd.to_datetime(df_display["date_utc"]).dt.strftime(
            "%Y-%m-%d %H:%M UTC"
        )
    if "success" in df_display.columns:
        df_display["success"] = df_display["success"].apply(
            lambda x: "✅ Yes"
            if x is True
            else ("❌ No" if x is False else "⏳ TBD/Upcoming")
        )
    st.dataframe(df_display, use_container_width=True, height=500)


def build_statistics(df: pd.DataFrame):
    """Calculates and displays various statistics derived from the launch data.

    This function renders a "Statistics" section in the Streamlit app, including:
    - Rocket success rates (%).
    - Number of launches by launch site.
    - A bar chart showing launch frequencies over time (monthly or yearly),
      allowing user selection for the time bucket.

    It relies on helper functions (`calculate_rocket_success`,
    `calculate_group_by_counts`, `calculate_time_bucket_counts`,
    `draw_metrics_horizontally`) to perform calculations and display metrics.

    Args:
        df (pd.DataFrame): A DataFrame containing filtered launch data.
                           It's expected to have a 'date_utc' column (or DatetimeIndex)
                           and columns specified by the calculation functions
                           (e.g., 'rocket_name', 'success', 'launchpad_name').
    """
    st.header("Statistics")

    st.subheader("Rocket Success Rate")
    rocket_success = calculate_rocket_success(
        df, rocket_name_column="rocket_name", success_column="success"
    )
    draw_metrics_horizontally(rocket_success, unit="%")

    st.subheader("Launches By Site")
    launches_by_launchpads = calculate_group_by_counts(
        df, group_by_column="launchpad_name"
    )
    draw_metrics_horizontally(launches_by_launchpads)

    st.subheader("Launch Frequencies")
    time_bucket_size_selection = st.segmented_control(
        label=None, options=["Month", "Year"], selection_mode="single", default="Month"
    )
    time_bucket_counts = calculate_time_bucket_counts(
        df, time_bucket="ME" if time_bucket_size_selection == "Month" else "YE"
    )
    fig = px.bar(
        time_bucket_counts,
        x=time_bucket_counts.index,
        y=time_bucket_counts,
        title=f"Launch Frequency by {time_bucket_size_selection}",
        labels={"y": "Launch Frequency", "date_utc": time_bucket_size_selection},
    )
    st.plotly_chart(fig)


def build_download_buttons(df: pd.DataFrame):
    """Renders download buttons in the Streamlit sidebar for CSV and JSON formats.

    This function takes a DataFrame (typically the one displayed to the user
    after filtering and formatting) and provides options to download it.

    Args:
        df (pd.DataFrame): The DataFrame to be made available for download.
                           This should ideally be the user-facing, formatted version of the data.
    """
    st.sidebar.header("💾 Download Filtered Launches")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "CSV",
            csv,
            "spacex_launches.csv",
            "text/csv",
            key="download-csv",
            use_container_width=True,
        )
    with col2:
        json = df.to_json(index=False, orient="records")
        st.download_button(
            "JSON",
            json,
            "spacex_launches.json",
            "application/json",
            key="download-json",
            use_container_width=True,
        )


def main():
    """Main function to run the SpaceX Launch Explorer Streamlit application.

    Sets up the page, connects to MongoDB, builds the UI (sidebar filters,
    data display, statistics, download options, webhook configuration),
    fetches and processes data based on user input, and displays results.
    """
    st.set_page_config(page_title="SpaceX Launch Explorer", layout="wide")
    st.title("🚀 SpaceX Launch Explorer")

    client = get_mongo_client()
    if client is None:
        return

    try:
        collection = client[MONGODB_DB_NAME][MONGODB_LAUNCHES_COLLECTION]
        if collection.count_documents({}) == 0:
            st.info(
                f"The '{MONGODB_LAUNCHES_COLLECTION}' collection in database '{MONGODB_DB_NAME}' appears to be empty. Reload the page when data is synced."
            )
            return
    except Exception as e:
        st.error(
            f"Error checking if collection '{MONGODB_LAUNCHES_COLLECTION}' is empty: {e}"
        )

    (
        start_date_val,
        end_date_val,
        selected_rockets,
        selected_status,
        selected_sites,
    ) = build_sidebar_filters(client)

    mongo_query = build_query(
        start_date_val, end_date_val, selected_rockets, selected_status, selected_sites
    )

    st.header("Filtered Launch Data")
    df = fetch_filtered_launches(client, mongo_query)
    if df.empty:
        st.info("No launches found for the selected filters!")
        return
    build_launches_table(df)
    build_statistics(df)
    build_download_buttons(df)

    st.sidebar.header("🪝 Webhook")
    webhooks_button = st.sidebar.button("Configure")
    if webhooks_button:
        webhooks_dialog(client)


if __name__ == "__main__":
    main()
