import pytest
import pandas as pd
from pandas.testing import assert_series_equal

from stats import calculate_time_bucket_counts


@pytest.fixture
def sample_datetime_df() -> pd.DataFrame:
    """Returns a sample DataFrame with a DatetimeIndex."""
    dates = pd.to_datetime(
        [
            "2023-01-15",
            "2023-01-20",  # Jan: 2
            "2023-02-10",  # Feb: 1
            "2023-03-05",
            "2023-03-25",
            "2023-03-30",  # Mar: 3
            "2024-05-10",
            "2024-05-15",  # May 2024: 2
            "2024-12-31",  # Dec 2024: 1
        ]
    )
    return pd.DataFrame({"value": range(len(dates))}, index=dates)


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Returns an empty DataFrame with a DatetimeIndex (important for type consistency)."""
    return pd.DataFrame(index=pd.to_datetime([]), columns=["A"], dtype=object)


@pytest.fixture
def empty_df_no_index() -> pd.DataFrame:
    """Returns an empty DataFrame with no index defined."""
    return pd.DataFrame()


@pytest.fixture
def non_datetime_index_df() -> pd.DataFrame:
    """Returns a DataFrame with a non-DatetimeIndex."""
    return pd.DataFrame({"value": [1, 2, 3]}, index=[1, 2, 3])


def test_empty_dataframe_with_datetime_index(empty_df):
    """Test with an empty DataFrame that has a DatetimeIndex."""
    result_me = calculate_time_bucket_counts(empty_df, "ME")
    expected = pd.Series(dtype=int)
    assert_series_equal(result_me, expected, check_dtype=True)
    assert result_me.empty

    result_ye = calculate_time_bucket_counts(empty_df, "YE")
    assert_series_equal(result_ye, expected, check_dtype=True)
    assert result_ye.empty


def test_empty_dataframe_no_index(empty_df_no_index):
    """Test with an empty DataFrame that has no index (or default RangeIndex)."""
    result_me = calculate_time_bucket_counts(empty_df_no_index, "ME")
    expected = pd.Series(dtype=int)
    assert_series_equal(result_me, expected, check_dtype=True)
    assert result_me.empty


def test_non_datetime_index(non_datetime_index_df, capfd):
    """Test with a DataFrame that does not have a DatetimeIndex."""
    result = calculate_time_bucket_counts(non_datetime_index_df, "ME")
    expected = pd.Series(dtype=int)
    assert_series_equal(result, expected, check_dtype=True)
    assert result.empty


def test_monthly_buckets(sample_datetime_df):
    """Test monthly time bucket counts."""
    result = calculate_time_bucket_counts(sample_datetime_df, "ME")

    expected_index_dates = pd.to_datetime(
        [
            "2023-01-31",
            "2023-02-28",
            "2023-03-31",
            "2023-04-30",
            "2023-05-31",
            "2023-06-30",
            "2023-07-31",
            "2023-08-31",
            "2023-09-30",
            "2023-10-31",
            "2023-11-30",
            "2023-12-31",
            "2024-01-31",
            "2024-02-29",
            "2024-03-31",
            "2024-04-30",
            "2024-05-31",
            "2024-06-30",
            "2024-07-31",
            "2024-08-31",
            "2024-09-30",
            "2024-10-31",
            "2024-11-30",
            "2024-12-31",
        ]
    )
    expected_index = pd.DatetimeIndex(expected_index_dates, freq="ME")

    expected_values = [
        2,
        1,
        3,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,  # 2023
        0,
        0,
        0,
        0,
        2,
        0,
        0,
        0,
        0,
        0,
        0,
        1,  # 2024
    ]
    expected_series = pd.Series(expected_values, index=expected_index, dtype=int)
    assert_series_equal(result, expected_series, check_dtype=True)


def test_yearly_buckets(sample_datetime_df):
    """Test yearly time bucket counts."""
    result = calculate_time_bucket_counts(sample_datetime_df, "YE")

    expected_index_dates = pd.to_datetime(["2023-12-31", "2024-12-31"])
    expected_index = pd.DatetimeIndex(expected_index_dates, freq="YE")
    expected_values = [6, 3]
    expected_series = pd.Series(expected_values, index=expected_index, dtype=int)
    assert_series_equal(result, expected_series, check_dtype=True)


def test_single_entry_monthly():
    """Test with a single entry for monthly bucketing."""
    df = pd.DataFrame({"value": [1]}, index=pd.to_datetime(["2023-07-10"]))
    result = calculate_time_bucket_counts(df, "ME")

    expected_index_dates = pd.to_datetime(["2023-07-31"])
    expected_index = pd.DatetimeIndex(expected_index_dates, freq="ME")
    expected_series = pd.Series([1], index=expected_index, dtype=int)
    assert_series_equal(result, expected_series, check_dtype=True)


def test_single_entry_yearly():
    """Test with a single entry for yearly bucketing."""
    df = pd.DataFrame({"value": [1]}, index=pd.to_datetime(["2023-07-10"]))
    result = calculate_time_bucket_counts(df, "YE")

    expected_index_dates = pd.to_datetime(["2023-12-31"])
    expected_index = pd.DatetimeIndex(expected_index_dates, freq="YE")
    expected_series = pd.Series([1], index=expected_index, dtype=int)
    assert_series_equal(result, expected_series, check_dtype=True)


def test_invalid_time_bucket_string(sample_datetime_df, capfd):
    """Test with an invalid time_bucket string that resample would reject."""
    result = calculate_time_bucket_counts(sample_datetime_df, "INVALID_RULE")  # type: ignore
    expected = pd.Series(dtype=int)
    assert_series_equal(result, expected, check_dtype=True)
    assert result.empty


def test_data_on_bucket_boundaries_monthly():
    """Test data points exactly on month-end boundaries."""
    dates = pd.to_datetime(["2023-01-31", "2023-02-01", "2023-02-28"])
    df = pd.DataFrame({"value": range(len(dates))}, index=dates)
    result = calculate_time_bucket_counts(df, "ME")

    expected_index_dates = pd.to_datetime(["2023-01-31", "2023-02-28"])
    expected_index = pd.DatetimeIndex(expected_index_dates, freq="ME")
    expected_values = [1, 2]
    expected_series = pd.Series(expected_values, index=expected_index, dtype=int)
    assert_series_equal(result, expected_series, check_dtype=True)


def test_data_on_bucket_boundaries_yearly():
    """Test data points exactly on year-end boundaries."""
    dates = pd.to_datetime(["2022-12-31", "2023-01-01", "2023-12-31"])
    df = pd.DataFrame({"value": range(len(dates))}, index=dates)
    result = calculate_time_bucket_counts(df, "YE")

    expected_index_dates = pd.to_datetime(["2022-12-31", "2023-12-31"])
    expected_index = pd.DatetimeIndex(expected_index_dates, freq="YE")
    expected_values = [1, 2]
    expected_series = pd.Series(expected_values, index=expected_index, dtype=int)
    assert_series_equal(result, expected_series, check_dtype=True)
