import pandas as pd

from API.EMInfraDomain import LocatieKenmerk


def format_locatie_kenmerk_lgc_2_wkt(locatie: LocatieKenmerk) -> str:
    """
    Format LocatieKenmerk as input to a WKT string as output
    Supported geometry formats: Point
    :param locatie: LocatieKenmerk
    :return:
    """
    locatie_data = getattr(locatie, "locatie", None)
    if not locatie_data or locatie_data.get("geometrie") is None:
        return None
    if locatie.locatie.get('_type') != 'punt':
        # implementation for other geometry types
        return None
    coordinaten = locatie.locatie.get('coordinaten')
    return f'POINT Z ({coordinaten.get("x")} {coordinaten.get("y")} {coordinaten.get("z", 0)})'


def parse_coordinates(wkt_geom: str) -> []:
    """
    Parse a wkt_geom string of a Point geometry to a list of integers. Coordinates are rounded
    :param wkt_geom of a Point-geometry
    :return: List of coordinates
    """
    if wkt_geom == 'nan' or pd.isna(wkt_geom):
        return None

    # Extract the numbers from the parentheses
    coordinates = wkt_geom.split('(')[1].split(')')[0].split()

    return [int(float(c)) for c in coordinates]


def geometries_are_identical(wkt_geom1, wkt_geom2) -> bool:
    """
    Compares two Points, wkt geometry. Returns true if they are identical.

    :param wkt_geom1:
    :param wkt_geom2:
    :return:
    """
    coordinates_1 = parse_coordinates(wkt_geom1)
    coordinates_2 = parse_coordinates(wkt_geom2)
    return coordinates_1 == coordinates_2
