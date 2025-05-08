import pytest
from datetime import datetime, timezone
from typing import Dict, Any
from prefect.testing.utilities import prefect_test_harness


from tasks.transform import transform_launches


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.fixture
def raw_launch_1() -> Dict[str, Any]:
    return {
        "id": "l1",
        "name": "Launch Alpha",
        "date_utc": "2023-01-15T10:00:00.000Z",
        "details": "Alpha mission details",
        "launchpad": "lp1",
        "rocket": "r1",
        "success": True,
        "upcoming": False,
    }


@pytest.fixture
def raw_launch_2() -> Dict[str, Any]:
    return {
        "id": "l2",
        "name": "Launch Beta",
        "date_utc": "2023-02-20T12:30:00+00:00",
        "details": "Beta mission details",
        "launchpad": "lp2",
        "rocket": "r2",
        "success": False,
        "upcoming": True,
    }


@pytest.fixture
def launchpad_1() -> Dict[str, Any]:
    return {"id": "lp1", "name": "Pad A", "full_name": "Launch Complex A"}


@pytest.fixture
def launchpad_2() -> Dict[str, Any]:
    return {"id": "lp2", "name": "Pad B", "full_name": "Launch Complex B"}


@pytest.fixture
def rocket_1() -> Dict[str, Any]:
    return {"id": "r1", "name": "Rocket X"}


@pytest.fixture
def rocket_2() -> Dict[str, Any]:
    return {"id": "r2", "name": "Rocket Y"}


def test_transform_launches_empty_raw_launches_list(launchpad_1, rocket_1):
    """Test with an empty list of raw_launches."""
    result = transform_launches(
        raw_launches=[], launchpads=[launchpad_1], rockets=[rocket_1]
    )
    assert result == []


def test_transform_launches_all_empty_inputs():
    """Test with all input lists being empty."""
    result = transform_launches(raw_launches=[], launchpads=[], rockets=[])
    assert result == []


def test_transform_launches_happy_path_single_launch(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test a single successful transformation."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1], launchpads=[launchpad_1], rockets=[rocket_1]
    )
    assert len(transformed_data) == 1
    expected_output = {
        "_id": "l1",
        "name": "Launch Alpha",
        "date_utc": datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        "details": "Alpha mission details",
        "launchpad_id": "lp1",
        "launchpad_name": "Pad A",
        "launchpad_fullname": "Launch Complex A",
        "rocket_id": "r1",
        "rocket_name": "Rocket X",
        "success": True,
        "upcoming": False,
    }
    assert transformed_data[0] == expected_output


def test_transform_launches_happy_path_multiple_launches(
    raw_launch_1, raw_launch_2, launchpad_1, launchpad_2, rocket_1, rocket_2
):
    """Test transformation of multiple launches with corresponding data."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1, raw_launch_2],
        launchpads=[launchpad_1, launchpad_2],
        rockets=[rocket_1, rocket_2],
    )
    assert len(transformed_data) == 2
    expected_output_1 = {
        "_id": "l1",
        "name": "Launch Alpha",
        "date_utc": datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        "details": "Alpha mission details",
        "launchpad_id": "lp1",
        "launchpad_name": "Pad A",
        "launchpad_fullname": "Launch Complex A",
        "rocket_id": "r1",
        "rocket_name": "Rocket X",
        "success": True,
        "upcoming": False,
    }
    expected_output_2 = {
        "_id": "l2",
        "name": "Launch Beta",
        "date_utc": datetime(2023, 2, 20, 12, 30, 0, tzinfo=timezone.utc),
        "details": "Beta mission details",
        "launchpad_id": "lp2",
        "launchpad_name": "Pad B",
        "launchpad_fullname": "Launch Complex B",
        "rocket_id": "r2",
        "rocket_name": "Rocket Y",
        "success": False,
        "upcoming": True,
    }
    assert transformed_data[0] == expected_output_1
    assert transformed_data[1] == expected_output_2


def test_transform_launches_missing_launchpad_key_in_raw_launch(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test when 'launchpad' key is missing in a raw launch."""
    launch_mod = raw_launch_1.copy()
    del launch_mod["launchpad"]

    transformed_data = transform_launches([launch_mod], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["launchpad_id"] is None
    assert item["launchpad_name"] is None
    assert item["launchpad_fullname"] is None
    assert item["rocket_id"] == "r1"


def test_transform_launches_launchpad_id_is_none(raw_launch_1, launchpad_1, rocket_1):
    """Test when 'launchpad' value is None in a raw launch."""
    launch_mod = raw_launch_1.copy()
    launch_mod["launchpad"] = None

    transformed_data = transform_launches([launch_mod], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["launchpad_id"] is None
    assert item["launchpad_name"] is None
    assert item["launchpad_fullname"] is None


def test_transform_launches_missing_rocket_key_in_raw_launch(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test when 'rocket' key is missing in a raw launch."""
    launch_mod = raw_launch_1.copy()
    del launch_mod["rocket"]

    transformed_data = transform_launches([launch_mod], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["rocket_id"] is None
    assert item["rocket_name"] is None
    assert item["launchpad_id"] == "lp1"


def test_transform_launches_rocket_id_is_none(raw_launch_1, launchpad_1, rocket_1):
    """Test when 'rocket' value is None in a raw launch."""
    launch_mod = raw_launch_1.copy()
    launch_mod["rocket"] = None

    transformed_data = transform_launches([launch_mod], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["rocket_id"] is None
    assert item["rocket_name"] is None


def test_transform_launches_launchpad_id_not_found(raw_launch_1, rocket_1):
    """Test when a launchpad ID from raw_launch is not in the launchpads list."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1], launchpads=[], rockets=[rocket_1]
    )
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["launchpad_id"] == "lp1"
    assert item["launchpad_name"] is None
    assert item["launchpad_fullname"] is None


def test_transform_launches_rocket_id_not_found(raw_launch_1, launchpad_1):
    """Test when a rocket ID from raw_launch is not in the rockets list."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1], launchpads=[launchpad_1], rockets=[]
    )
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["rocket_id"] == "r1"
    assert item["rocket_name"] is None


def test_transform_launches_launchpad_data_missing_attributes(raw_launch_1, rocket_1):
    """Test when a found launchpad dictionary is missing 'name' or 'full_name'."""
    incomplete_launchpad = {"id": "lp1"}
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1],
        launchpads=[incomplete_launchpad],
        rockets=[rocket_1],
    )
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["launchpad_id"] == "lp1"
    assert item["launchpad_name"] is None
    assert item["launchpad_fullname"] is None


def test_transform_launches_rocket_data_missing_attributes(raw_launch_1, launchpad_1):
    """Test when a found rocket dictionary is missing 'name'."""
    incomplete_rocket = {"id": "r1"}
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1],
        launchpads=[launchpad_1],
        rockets=[incomplete_rocket],
    )
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["rocket_id"] == "r1"
    assert item["rocket_name"] is None


def test_transform_launches_missing_date_utc_key(raw_launch_1, launchpad_1, rocket_1):
    """Test when 'date_utc' key is missing in raw_launch."""
    launch_mod = raw_launch_1.copy()
    del launch_mod["date_utc"]

    transformed_data = transform_launches([launch_mod], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    assert transformed_data[0]["date_utc"] is None


def test_transform_launches_missing_optional_fields_in_raw_launch(
    launchpad_1, rocket_1
):
    """Test when optional fields like 'details', 'success', 'name' are missing."""
    minimal_launch = {
        "id": "min_l1",
        "launchpad": "lp1",
        "rocket": "r1"
        # Missing: name, date_utc, details, success, upcoming
    }
    transformed_data = transform_launches([minimal_launch], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["_id"] == "min_l1"
    assert item["name"] is None
    assert item["date_utc"] is None
    assert item["details"] is None
    assert item["success"] is None
    assert item["upcoming"] is None
    assert item["launchpad_name"] == "Pad A"
    assert item["rocket_name"] == "Rocket X"


def test_transform_launches_raw_launch_missing_id_field(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test when 'id' field is missing from a raw_launch item."""
    launch_mod = raw_launch_1.copy()
    del launch_mod["id"]

    transformed_data = transform_launches([launch_mod], [launchpad_1], [rocket_1])
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["_id"] is None
    assert item["name"] == "Launch Alpha"


def test_transform_launches_invalid_item_in_raw_launches(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test when raw_launches list contains non-dictionary items."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1, None, "not a dict", 123],
        launchpads=[launchpad_1],
        rockets=[rocket_1],
    )
    assert len(transformed_data) == 1
    assert transformed_data[0]["_id"] == "l1"


def test_transform_launches_invalid_item_in_launchpads(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test when launchpads list contains items not suitable for the map (non-dict or no id)."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1],
        launchpads=[launchpad_1, None, "not a dict", {"name": "Pad C (no id)"}],
        rockets=[rocket_1],
    )
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["launchpad_id"] == "lp1"
    assert item["launchpad_name"] == "Pad A"


def test_transform_launches_invalid_item_in_rockets(
    raw_launch_1, launchpad_1, rocket_1
):
    """Test when rockets list contains items not suitable for the map (non-dict or no id)."""
    transformed_data = transform_launches(
        raw_launches=[raw_launch_1],
        launchpads=[launchpad_1],
        rockets=[rocket_1, None, "not a dict", {"name": "Rocket Z (no id)"}],
    )
    assert len(transformed_data) == 1
    item = transformed_data[0]
    assert item["rocket_id"] == "r1"
    assert item["rocket_name"] == "Rocket X"
