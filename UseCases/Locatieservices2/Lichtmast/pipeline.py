import logging

import pandas as pd

from name_parser import parse_lichtmast_naam, is_full_match
from utils.spatial import point_in_polygons, load_gemeente_to_gdf
from utils.wkt_geometry_helpers import (
    coordinates_2_wkt, get_euclidean_distance_wkt, generate_osm_link, parse_coordinates
)


def enrich_assets(df: pd.DataFrame, ls2, add_osm=False, add_prov=False):
    # vectorized name → derived‐WKT
    def _derive_wkt(name):
        if not is_full_match(name): return None
        pos, ident8, ops = parse_lichtmast_naam(name)
        try:
            geom = ls2.zoek_puntlocatie_via_wegsegment(ident8=ident8, opschrift=ops)
            return coordinates_2_wkt(geom.geometry.coordinates)
        except Exception as e:
            logging.debug(f'Exception occured: {e}')
            return None

    def _get_name_from_geometry(geometry: str) -> str | None:
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
            afgeleide_locatie = ls2.zoek_puntlocatie_via_xy(x=x, y=y, zoekafstand=100)
            naam = f'{afgeleide_locatie.relatief.referentiepunt.wegnummer.nummer}_{afgeleide_locatie.relatief.referentiepunt.opschrift}_{afgeleide_locatie.relatief.afstand}'

        except Exception as e:
            logging.critical(f'Locatieservices kon de locatie niet afleiden. Foutmelding: {e}')
            naam = None

        return naam

    df['geometrie_afgeleid'] = df['naam'].map(_derive_wkt)

    # vectorized distance (series→series)
    df['afstand'] = df.apply(lambda row: get_euclidean_distance_wkt(row['geometry'], row['geometrie_afgeleid']), axis=1)

    df["naam_afgeleid"] = df.apply(lambda row: _get_name_from_geometry(geometry=row['geometry']), axis = 1)

    if add_osm:
        df['osm_link'] = df['geometrie_afgeleid'].map(generate_osm_link)

    if add_prov:
        gemeenten = load_gemeente_to_gdf('gemeente.json')
        df['provincie'] = df['geometry'].map(
            lambda w: point_in_polygons(w, gemeenten, 'provincie')
        )
        df['gemeente'] = df['geometry'].map(
            lambda w: point_in_polygons(w, gemeenten, 'gemeente')
        )

    df["eminfra"] = 'https://apps.mow.vlaanderen.be/eminfra/assets/' + df["assetId.identificator"].str[:36]

    return df