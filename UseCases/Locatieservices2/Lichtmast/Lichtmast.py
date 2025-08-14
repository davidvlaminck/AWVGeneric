import logging
import re
import pathlib
import json

from typing import Optional
import pandas as pd
import geopandas as gpd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from pathlib import Path

from API.Locatieservices2Client import Locatieservices2Client
from utils.locatieservice_helpers import convert_ident8
from utils.wkt_geometry_helpers import coordinates_2_wkt, get_euclidean_distance_wkt, generate_osm_link, \
    parse_coordinates
from shapely.geometry import Point
from shapely import wkt
from shapely.wkt import loads as wkt_loads
from shapely.errors import ShapelyError


# Regex pattern for Lichtmast name matching
LICHTMAST_REGEX_PATTERN = r'[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*\.[P]\d*'


def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'


def is_full_match(name: str, pattern: str = LICHTMAST_REGEX_PATTERN) -> bool:
    """
    Returns True if the entire string matches the given regex pattern.

    :type name: str
    :param name: The input string to test.
    :type pattern: str
    :param pattern: The regular expression pattern.
    :return: Boolean indicating if full match occurred.
    """
    return re.fullmatch(pattern, name) is not None


def parse_lichtmast_naam(naam: str) -> tuple[str, str, str]:
    """
    Ontleden (parse) van de naam van de lichtmast in verschillende componenten: positie_rijwel, ident8 en opschrift

    :param naam:
    :return: Tuple met 3 elementen: positie van de rijweg, ident8 en opschrift.
    """
    logging.info('Extraheer het eerste deel van een string')
    if not (match_lichtmast_naam_basis := re.match(pattern='[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*', string=naam)):
        raise ValueError('De naam van de Lichtmast kan niet worden afgeleid op basis van de input.')
    lichtmast_naam_basis = match_lichtmast_naam_basis[0]

    logging.info('Extraheer de aanduiding van de richting (NMPX), alsook de indexpositie in de string')
    if not (
    match := re.search(pattern=r"(?<=[0-9])([MNPX])(?=[0-9])", string=lichtmast_naam_basis, flags=re.IGNORECASE)):
        raise ValueError('De positie van de rijweg kan niet worden afgeleid op basis van de naam van de Lichtmast')
    index = match.start()
    positie_rijweg = match.group()

    ident8_raw = lichtmast_naam_basis[:index]
    opschrift = str(round(float(lichtmast_naam_basis[index + 1:]), 1))  # afronden tot op 1 decimaal getal

    ident8 = convert_ident8(ident8=ident8_raw, direction=positie_rijweg)

    return positie_rijweg, ident8, opschrift


def get_wkt_from_name(naam: str) -> str | None:
    """
    Afleiden van de WKT-string op basis van de naam.

    :param naam: asset naam
    :return: WKT-string
    """
    if naam is None or not is_full_match(name=naam, pattern=LICHTMAST_REGEX_PATTERN):
        logging.debug(f'Naam {naam} is None of volgt niet de naamconventie van een Lichtmast.')
        return None

    positie_rijweg, ident8, opschrift = parse_lichtmast_naam(naam=naam)
    logging.debug('Zoek puntlocatie via wegsegment.')
    logging.debug(f'ident8: {ident8}')
    logging.debug(f'opschrift: {opschrift}')

    try:
        wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_wegsegment(ident8=ident8, opschrift=opschrift)
        wkt_geom = coordinates_2_wkt(wegsegment_puntlocatie.geometry.coordinates)

    except Exception as e:
        logging.critical(f'Locatieservices kon de locatie niet ophalen. Foutmelding: {e}')
        wkt_geom = None

    return wkt_geom

def get_name_from_geometry(geometry: str) -> str | None:
    """
    Afleiden van de naam op basis van de WKT-string.

    :param geometry: WKT-string
    :return: naam
    """
    if geometry is None:
        logging.debug('Geometry is None')
        return None

    try:
        coordinates = parse_coordinates(wkt_geom=geometry)
        x = coordinates[0]
        y = coordinates[1]
        afgeleide_locatie = ls2_client.zoek_puntlocatie_via_xy(x=x, y=y, zoekafstand=100)
        naam = f'{afgeleide_locatie.relatief.referentiepunt.wegnummer.nummer}_{afgeleide_locatie.relatief.referentiepunt.opschrift}_{afgeleide_locatie.relatief.afstand}'

    except Exception as e:
        logging.critical(f'Locatieservices kon de locatie niet afleiden. Foutmelding: {e}')
        naam = None

    return naam

def load_gemeente_to_gdf(filename: str = 'gemeente.json', crs: str = "EPSG:31370", target_crs: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Load a JSON file with WKT geometries into a GeoDataFrame.

    Args:
        json_path (str): Path to the JSON file.
        crs (str): The CRS of the input WKT geometries (default 'EPSG:31370').
        target_crs (Optional[str]): CRS to reproject the GeoDataFrame to (e.g. 'EPSG:4326'). If None, no reprojection.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame with geometries and attributes, skipping invalid/missing WKT.
    """
    # Load JSON file
    with pathlib.Path(filename).open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract main list (assuming structure like {"gemeente": [ ... ]})
    records = data.get("gemeente", [])
    df = pd.DataFrame(records)

    # Convert WKT strings to shapely geometries, skipping invalid ones
    def safe_wkt_load(wkt_str):
        if not wkt_str or not isinstance(wkt_str, str):
            return None
        try:
            return wkt.loads(wkt_str)
        except ShapelyError:
            return None

    df["geometry"] = df["geom"].apply(safe_wkt_load)

    # Drop rows where geometry could not be created
    # df = df.dropna(subset=["geometry"])

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df.drop(columns=["geom"]), geometry="geometry", crs=crs)

    # Reproject if requested
    if target_crs:
        gdf = gdf.to_crs(target_crs)

    return gdf


def point_in_polygons(point_wkt: str, polygons_gdf: gpd.GeoDataFrame, name_column: str = 'provincie') -> str | None:
    """
    Check if a point (given as WKT) lies within any polygon in the GeoDataFrame.
    Returns the polygon name if found, else None.

    :param point_wkt: WKT string of the point, e.g., "POINT (x y)"
    :param polygons_gdf: GeoDataFrame with at least columns 'name' and 'geometry'
    """
    # Convert WKT to Shapely Point
    try:
        point = wkt_loads(point_wkt)
    except Exception as e:
        return None
        # raise ValueError(f"Invalid WKT point: {point_wkt}") from e

    if not isinstance(point, Point):
        return None
        # raise ValueError("Provided WKT is not a valid Point geometry.")

    # Vectorized spatial check using GeoDataFrame
    mask = polygons_gdf.geometry.contains(point)
    match = polygons_gdf[mask]

    return None if match.empty else match.iloc[0][f"{name_column}"]

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    logging.info(
        'Analyse van de locatie van de Lichtmasten.\nAfleiden van de locatie op basis van de naam van de Lichtmast, door gebruik te maken van de Locatieservice2 API.')
    settings_path = load_settings()

    ls2_client = Locatieservices2Client(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    filepath_excel_input = Path().home() / 'Downloads' / 'Lichtmast' / 'lichtmast_zonder_afgeleide_locatie' / 'DA-2025-46771_export.xlsx'
    filepath_excel_output = Path().home() / 'Downloads' / 'Lichtmast' / 'lichtmast_zonder_afgeleide_locatie' / 'DA-2025-XXXXX_import.xlsx'

    add_hyperlink_eminfra = True
    add_hyperlink_osm = False
    add_provincie = True

    usecols = ['typeURI', 'assetId.identificator', 'naam', 'naampad', 'toestand', 'geometry']
    df_assets = pd.read_excel(filepath_excel_input, sheet_name='Lichtmast', usecols=usecols)
    df_assets["naam"] = df_assets["naam"].apply(str)

    logging.info('Afleiden geometrie op basis van de naam')
    df_assets["geometrie_afgeleid"] = df_assets["naam"].apply(lambda naam: get_wkt_from_name(naam=naam))

    logging.info('Afleiden locatie op basis van de geometrie')
    df_assets["naam_afgeleid"] = df_assets["geometry"].apply(lambda geometry: get_name_from_geometry(geometry=geometry))

    logging.info('Berekenen (euclidische) afstand tussen geometrie en afgeleide geometrie')
    df_assets['afstand'] = df_assets.apply(lambda row: get_euclidean_distance_wkt(row['geometry'], row['geometrie_afgeleid']), axis=1)

    if add_hyperlink_eminfra:
        logging.info('Toevoegen hyperlink naar de toepassing em-infra.')
        df_assets["eminfra"] = 'https://apps.mow.vlaanderen.be/eminfra/assets/' + df_assets["assetId.identificator"].str[
                                                                              :36]
    if add_hyperlink_osm:
        logging.info('Toevoegen hyperlink naar Openstreetmap (OSM)')
        df_assets[["osm_link"]] = df_assets["geometrie_referentiepunt"].apply(lambda wkt: generate_osm_link(wkt))

    if add_provincie:
        df_gemeenten = load_gemeente_to_gdf(filename='gemeente.json')

        df_assets[["gemeente"]] = df_assets.apply(lambda row: point_in_polygons(point_wkt=row['geometry'], polygons_gdf=df_gemeenten, name_column='gemeente'), axis = 1)
        df_assets[["provincie"]] = df_assets.apply(lambda row: point_in_polygons(point_wkt=row['geometry'], polygons_gdf=df_gemeenten, name_column='provincie'), axis = 1)

    logging.debug(f'Write pandas dataframe to Excel: {filepath_excel_output}')
    df_assets.to_excel(filepath_excel_output, sheet_name='Lichtmast', freeze_panes=[1, 2], index=False)
