import math

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

def coordinates_2_wkt(coords: list[float]) -> str:
    """
    Transform a list of 2 (or 3) coordinates into a WKT Point Z
    :param coords:
    :return:
    """
    if len(coords) == 2:
        coords.append(0.0)
    return f'POINT Z({coords[0]} {coords[1]} {coords[2]})'

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

def get_euclidean_distance_coordinates(x1: float, y1: float, x2: float, y2: float) -> float:
    """
    Returns the Euclidean distance between 2 points

    :param x1:
    :param y1:
    :param x2:
    :param y2:
    :return:
    """
    return math.sqrt((x2-x1)**2 + (y2-y1)**2)

def get_euclidean_distance_wkt(wkt1: str, wkt2: str) -> float:
    """
    Returns the Euclidean distance between 2 wkt Point geometries

    :param wkt1:
    :param wkt2:
    :return:
    """
    coords1 = parse_coordinates(wkt1)
    coords2 = parse_coordinates(wkt2)
    return get_euclidean_distance_coordinates(x1=coords1[0], y1=coords1[1], x2=coords2[0], y2=coords2[1])