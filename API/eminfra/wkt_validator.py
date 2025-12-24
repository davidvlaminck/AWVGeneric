from shapely import wkt
from shapely.errors import WKTReadingError

def is_valid_wkt(wkt_string: str) -> bool:
    """
    Validate whether a string is a valid OGC WKT geometry.

    :param wkt_string: Geometry in WKT format
    :return: True if valid WKT, False otherwise
    """
    if not wkt_string or not isinstance(wkt_string, str):
        return False
    try:
        geom = wkt.loads(wkt_string)
        return geom.is_valid
    except (WKTReadingError, Exception):
        return False