import pandas as pd
import numpy as np
from stats import calculate_rocket_success


def test_simple():
    """Tests a simple case."""
    data = {
        "rocket_name": ["Falcon 9", "Falcon 9", "Falcon Heavy", "Falcon 9"],
        "success": [True, True, False, False],
    }
    launches_df = pd.DataFrame(data)
    expected = {"Falcon 9": 66.67, "Falcon Heavy": 0.0}
    assert calculate_rocket_success(launches_df) == expected


def test_empty_dataframe():
    """Tests with an empty DataFrame."""
    launches_df = pd.DataFrame(columns=["rocket_name", "success"])
    assert calculate_rocket_success(launches_df) == {}


def test_missing_rocket_name_column():
    """Tests when the rocket_name_column is missing."""
    data = {"vehicle_name": ["Vostok"], "success": [True]}
    launches_df = pd.DataFrame(data)
    assert calculate_rocket_success(launches_df) == {}


def test_missing_success_column():
    """Tests when the success_column is missing."""
    data = {"rocket_name": ["Ariane 5"], "outcome": ["Success"]}
    launches_df = pd.DataFrame(data)
    assert calculate_rocket_success(launches_df) == {}


def test_all_successes():
    """Tests a scenario where all launches for a rocket are successful."""
    data = {
        "rocket_name": ["Atlas V", "Atlas V", "Atlas V"],
        "success": [True, True, True],
    }
    launches_df = pd.DataFrame(data)
    expected = {"Atlas V": 100.0}
    assert calculate_rocket_success(launches_df) == expected


def test_all_failures():
    """Tests a scenario where all launches for a rocket fail."""
    data = {
        "rocket_name": ["N1", "N1", "N1"],
        "success": [False, False, False],
    }
    launches_df = pd.DataFrame(data)
    expected = {"N1": 0.0}
    assert calculate_rocket_success(launches_df) == expected


def test_numeric_success_column():
    """Tests with numeric values (0/1) in the success column."""
    data = {
        "rocket_name": ["Soyuz", "Soyuz", "Soyuz", "Soyuz"],
        "success": [1, 1, 0, 1],
    }
    launches_df = pd.DataFrame(data)
    expected = {"Soyuz": 75.0}
    assert calculate_rocket_success(launches_df) == expected


def test_custom_column_names():
    """Tests with custom names for rocket_name and success columns."""
    data = {
        "vehicle": ["Electron", "Electron", "Starship"],
        "mission_outcome": [True, False, False],
    }
    launches_df = pd.DataFrame(data)
    expected = {"Electron": 50.0, "Starship": 0.0}
    result = calculate_rocket_success(
        launches_df, rocket_name_column="vehicle", success_column="mission_outcome"
    )
    assert result == expected


def test_single_launch_success():
    """Tests a single successful launch."""
    data = {"rocket_name": ["Long March 5"], "success": [True]}
    launches_df = pd.DataFrame(data)
    expected = {"Long March 5": 100.0}
    assert calculate_rocket_success(launches_df) == expected


def test_single_launch_failure():
    """Tests a single failed launch."""
    data = {"rocket_name": ["Vega"], "success": [False]}
    launches_df = pd.DataFrame(data)
    expected = {"Vega": 0.0}
    assert calculate_rocket_success(launches_df) == expected


def test_rounding_precision():
    """Tests the rounding to two decimal places."""
    data = {
        "rocket_name": ["RocketA", "RocketA", "RocketA"],
        "success": [True, False, False],
    }
    launches_df = pd.DataFrame(data)
    expected = {"RocketA": 33.33}
    assert calculate_rocket_success(launches_df) == expected

    data_b = {
        "rocket_name": [
            "RocketB",
            "RocketB",
            "RocketB",
            "RocketB",
            "RocketB",
            "RocketB",
        ],
        "success": [True, False, False, False, False, False],
    }
    launches_df_b = pd.DataFrame(data_b)
    expected_b = {"RocketB": 16.67}
    assert calculate_rocket_success(launches_df_b) == expected_b


def test_success_column_with_nan_values_boolean():
    """Tests how NaN values in a boolean success column are handled (should be ignored by mean)."""
    data = {
        "rocket_name": ["RocketC", "RocketC", "RocketC", "RocketC"],
        "success": [True, False, pd.NA, True],
    }
    launches_df = pd.DataFrame(data).astype({"success": "boolean"})
    expected = {"RocketC": 66.67}
    assert calculate_rocket_success(launches_df) == expected


def test_success_column_with_nan_values_float():
    """Tests how NaN values in a float success column are handled (should be ignored by mean)."""
    data = {
        "rocket_name": ["RocketD", "RocketD", "RocketD", "RocketD"],
        "success": [1.0, 0.0, np.nan, 1.0],
    }
    launches_df = pd.DataFrame(data)
    expected = {"RocketD": 66.67}
    assert calculate_rocket_success(launches_df) == expected


def test_invalid_data_in_success_column(capsys):
    """Tests when success column contains non-boolean/non-numeric strings."""
    data = {
        "rocket_name": ["Delta IV", "Delta IV"],
        "success": ["Succeeded", "Failed"],
    }
    launches_df = pd.DataFrame(data)
    result = calculate_rocket_success(launches_df)
    assert result == {}


def test_rocket_name_column_with_nan_values():
    """
    Tests how NaN values in the rocket_name_column are handled.
    """
    data = {
        "rocket_name": ["Falcon 9", np.nan, "Falcon 9", np.nan, "Atlas V"],
        "success": [True, False, False, True, True],
    }
    launches_df = pd.DataFrame(data)
    result = calculate_rocket_success(launches_df)

    assert len(result) == 2
    assert result.get("Falcon 9") == 50.0
    assert result.get("Atlas V") == 100.0


def test_mixed_boolean_and_numeric_success():
    """Tests if mixed boolean and numeric (0/1) success values are handled."""
    data = {
        "rocket_name": ["MixedRocket", "MixedRocket", "MixedRocket", "MixedRocket"],
        "success": [True, 0, 1, False],
    }
    launches_df = pd.DataFrame(data)
    expected = {"MixedRocket": 50.0}
    assert calculate_rocket_success(launches_df) == expected
