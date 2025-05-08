import pytest
import pandas as pd
import numpy as np

from stats import calculate_group_by_counts


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provides a sample DataFrame for testing."""
    data = {
        "launchpad_name": ["Site A", "Site B", "Site A", "Site C", "Site B", "Site A"],
        "status": ["Success", "Failure", "Success", "Success", "Success", "Failure"],
        "rockets": [1, 2, 1, 3, 2, 1],
    }
    return pd.DataFrame(data)


def test_empty_dataframe():
    """Test with an empty DataFrame."""
    result = calculate_group_by_counts(pd.DataFrame(), "any_column")
    assert result == {}


def test_column_not_exists(sample_dataframe: pd.DataFrame):
    """Test when the group_by_column does not exist in the DataFrame."""
    result = calculate_group_by_counts(sample_dataframe, "non_existent_column")
    assert result == {}


def test_basic_grouping_string_column(sample_dataframe: pd.DataFrame):
    """Test basic grouping on a string column."""
    result = calculate_group_by_counts(sample_dataframe, "launchpad_name")
    expected = {"Site A": 3, "Site B": 2, "Site C": 1}
    assert result == expected


def test_grouping_another_string_column(sample_dataframe: pd.DataFrame):
    """Test grouping on a different string column."""
    result = calculate_group_by_counts(sample_dataframe, "status")
    expected = {"Success": 4, "Failure": 2}
    assert result == expected


def test_grouping_numeric_column(sample_dataframe: pd.DataFrame):
    """Test grouping on a numeric column (keys in dict will be numbers)."""
    result = calculate_group_by_counts(sample_dataframe, "rockets")
    expected = {1: 3, 2: 2, 3: 1}
    assert result == expected


def test_single_group():
    """Test with a column that has only one unique value after filtering."""
    df_single_group = pd.DataFrame({"category": ["X", "X", "X"], "value": [1, 2, 3]})
    result = calculate_group_by_counts(df_single_group, "category")
    expected = {"X": 3}
    assert result == expected


def test_all_unique_values():
    """Test with a column where all values are unique."""
    df_all_unique = pd.DataFrame({"id": [101, 102, 103], "data": ["a", "b", "c"]})
    result = calculate_group_by_counts(df_all_unique, "id")
    expected = {101: 1, 102: 1, 103: 1}
    assert result == expected


def test_dataframe_with_nan_in_group_by_column():
    """Test DataFrame with NaN values in the group_by_column."""
    data_with_nan = {
        "category": ["A", np.nan, "B", "A", np.nan, "C"],
        "value": [1, 2, 3, 4, 5, 6],
    }
    df = pd.DataFrame(data_with_nan)
    result = calculate_group_by_counts(df, "category")
    assert len(result) == 3
    assert result.get("A") == 2
    assert result.get("B") == 1
    assert result.get("C") == 1


def test_single_row_dataframe():
    """Test with a DataFrame containing only one row."""
    df_single_row = pd.DataFrame({"name": ["Alice"], "age": [30]})
    result = calculate_group_by_counts(df_single_row, "name")
    expected = {"Alice": 1}
    assert result == expected
