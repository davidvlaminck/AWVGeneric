import json
import pathlib
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
from shapely.errors import ShapelyError

def load_gemeente_to_gdf(path: str, crs="EPSG:31370", target_crs=None) -> gpd.GeoDataFrame:
    data = json.loads(pathlib.Path(path).read_text(encoding="utf8"))
    df = pd.DataFrame(data.get("gemeente", []))
    def _safe(wkt_str):
        try: return wkt.loads(wkt_str)
        except ShapelyError: return None
    df['geometry'] = df['geom'].map(_safe)
    gdf = gpd.GeoDataFrame(df.drop(columns=['geom']), geometry='geometry', crs=crs)
    return gdf.to_crs(target_crs) if target_crs else gdf

def point_in_polygons(point_wkt: str, gdf: gpd.GeoDataFrame, col: str) -> str|None:
    try:
        pt = wkt.loads(point_wkt)
        if not isinstance(pt, Point):
            return None
        hits = gdf[gdf.geometry.contains(pt)]
        return hits.iloc[0][col] if not hits.empty else None
    except Exception:
        return None