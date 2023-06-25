import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.utilities import calculate_distance


def test_calculate_distance():
    # Test case 0: Random latitude and longitude difference
    row = {
        'latitude': 45.256245,
        'longitude': 23.241644,
        'latitude_next': 41.000901,
        'longitude_next': 43.668719
    }
    expected_distance = 1719
    distance = round(calculate_distance(row))
    assert distance == expected_distance


def test_multiple_distances():
    # Test case 1: Zero latitude and longitude difference
    row = {
        'latitude': 45.256245,
        'longitude': 23.241644,
        'latitude_next': 45.256245,
        'longitude_next': 23.241644
    }
    expected_distance = 0
    distance = round(calculate_distance(row))
    assert distance == expected_distance

    # Test case 2: Large difference in latitude, no longitude difference
    row = {
        'latitude': 10.123456,
        'longitude': 20.987654,
        'latitude_next': 60.123456,
        'longitude_next': 20.987654
    }
    expected_distance = 5560
    distance = round(calculate_distance(row))
    assert distance == expected_distance

    # Test case 3: Large difference in longitude, no latitude difference
    row = {
        'latitude': 30.987654,
        'longitude': 40.123456,
        'latitude_next': 30.987654,
        'longitude_next': 140.123456
    }
    expected_distance = 9129
    distance = round(calculate_distance(row))
    assert distance == expected_distance

    # Test case 4: Random latitude and longitude difference
    row = {
        'latitude': 50.123456,
        'longitude': -40.987654,
        'latitude_next': 55.678901,
        'longitude_next': -36.543210
    }
    expected_distance = 686
    distance = round(calculate_distance(row))
    assert distance == expected_distance
