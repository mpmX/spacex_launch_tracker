from typing import Literal, Dict
import pandas as pd


def calculate_rocket_success(
    launches: pd.DataFrame,
    rocket_name_column: str = "rocket_name",
    success_column: str = "success",
) -> Dict[str, float]:
    """Calculates the success rate for each rocket as a percentage.

    The success rate is determined by grouping the launches DataFrame by the
    specified `rocket_name_column` and then calculating the mean of the
    `success_column` for each group. The `success_column` is expected
    to contain boolean values (True for success, False for failure) or
    numeric values where 1 represents success and 0 represents failure.
    The result is multiplied by 100 and rounded to two decimal places.

    Args:
        launches (pd.DataFrame): DataFrame containing launch data. Must include
                                 the `rocket_name_column` and `success_column`.
        rocket_name_column (str, optional): The name of the column in `launches`
                                            that contains rocket names.
                                            Defaults to "rocket_name".
        success_column (str, optional): The name of the column in `launches` that
                                        indicates launch success (e.g., boolean or 0/1).
                                        Defaults to "success".

    Returns:
        Dict[str, float]: A dictionary where keys are rocket names (str) and
                          values are their success rates (float, as percentages
                          rounded to two decimal places). Returns an empty
                          dictionary if the input DataFrame is empty or if
                          the specified columns are not present/valid.

    Example:
        If launches is:
            rocket_name success
        0   Falcon 9    True
        1   Falcon 9    True
        2   Falcon Heavy False
        3   Falcon 9    False

        Result would be: {'Falcon 9': 66.67, 'Falcon Heavy': 0.0}
    """
    if (
        launches.empty
        or rocket_name_column not in launches.columns
        or success_column not in launches.columns
    ):
        return {}
    try:
        success_rates = (
            launches.groupby(rocket_name_column)[success_column].mean().astype(float)
            * 100
        ).round(2)
        return success_rates.to_dict()
    except Exception as e:
        print(f"Error calculating rocket success: {e}")
        return {}


def calculate_group_by_counts(df: pd.DataFrame, group_by_column: str) -> Dict[str, int]:
    """Calculates the count of items for each unique value in a specified column.

    This function groups the input DataFrame by the `group_by_column` and
    then counts the number of occurrences (rows) within each group.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        group_by_column (str): The name of the column to group by and count unique values.

    Returns:
        Dict[str, int]: A dictionary where keys are the unique values from the
                        `group_by_column` and values are their corresponding counts.
                        Returns an empty dictionary if the DataFrame is empty or
                        the column doesn't exist.

    Example:
        If df is:
            launchpad_name  status
        0   Site A          Success
        1   Site B          Failure
        2   Site A          Success

        And group_by_column is "launchpad_name",
        Result would be: {'Site A': 2, 'Site B': 1}
    """
    if df.empty or group_by_column not in df.columns:
        return {}
    return df.groupby(group_by_column).size().to_dict()


def calculate_time_bucket_counts(
    df: pd.DataFrame, time_bucket: Literal["ME", "YE"]
) -> pd.Series:
    """Calculates the number of occurrences (e.g., launches) within specified time buckets.

    This function resamples the DataFrame based on its DatetimeIndex and counts
    the number of entries in each time period (bucket). The time buckets can be
    Month-End ("ME") or Year-End ("YE").

    Args:
        df (pd.DataFrame): The DataFrame to process. It **must** have a
                           `DatetimeIndex`.
        time_bucket (Literal["ME", "YE"]): The resampling frequency.
            "ME" for Month-End frequency.
            "YE" for Year-End frequency.

    Returns:
        pd.Series: A pandas Series where the index represents the resampled
                   time periods (e.g., end of each month or year) and the
                   values are the counts of occurrences within each period.
                   Returns an empty Series if the input DataFrame is empty,
                   does not have a DatetimeIndex, or if resampling fails.
    """
    if df.empty:
        return pd.Series(dtype=int)
    if not isinstance(df.index, pd.DatetimeIndex):
        print("Error: DataFrame must have a DatetimeIndex for time bucket counts.")
        return pd.Series(dtype=int)

    try:
        return df.resample(rule=time_bucket).size()
    except Exception as e:
        print(f"Error during resampling: {e}")
        return pd.Series(dtype=int)
