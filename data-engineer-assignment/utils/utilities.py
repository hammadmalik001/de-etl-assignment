from math import sin, cos, acos, radians


def calculate_distance(row):
    """
    Calculate the distance between two coordinates.

    This function takes a row containing latitude and longitude values
    for two points and calculates the distance between them.

    :param row: A dictionary object containing the
    latitude and longitude values
    :return: The calculated distance in kilometers.
    """
    lat1, lon1 = radians(row['latitude']), radians(row['longitude'])
    lat2, lon2 = radians(row['latitude_next']), radians(row['longitude_next'])
    distance = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2)
                    * cos(lon2 - lon1)) * 6371
    return distance
