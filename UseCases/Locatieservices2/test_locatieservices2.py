import logging
import re

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from pathlib import Path

from API.Locatieservices2Client import Locatieservices2Client
from utils.locatieservice_helpers import convert_ident8
from utils.wkt_geometry_helpers import coordinates_2_wkt, get_euclidean_distance_wkt


def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def is_full_match(name: str, pattern: str = '[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*\.[P]\d*') -> bool:
    """
    Returns True if the entire string matches the given regex pattern.

    :type name: str
    :param name: The input string to test.
    :type pattern: str
    :param pattern: The regular expression pattern.
    :return: Boolean indicating if full match occurred.
    """
    return re.fullmatch(pattern, name) is not None


def parse_lichtmast_naam(naam: str):
    logging.info('Extraheer het eerste deel van een string')
    lichtmast_naam_basis = re.match(pattern='[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*', string=naam)[0]

    logging.info('Extraheer de aanduiding van de richting (NMPX), alsook de indexpositie in de string')
    match = re.search(pattern=r"(?<=[0-9])([MNPX])(?=[0-9])", string=lichtmast_naam_basis, flags=re.IGNORECASE)
    index = match.start()
    positie_rijweg = match.group()

    ident8_raw = lichtmast_naam_basis[:index]
    opschrift = lichtmast_naam_basis[index:]

    # to implement: functie om ident8_raw naar een ident8 van exact 8 digits om te zetten?
    ident8 = convert_ident8(ident8=ident8_raw, direction=positie_rijweg)

    return positie_rijweg, ident8, opschrift


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Test Locatieservices2 (LS2)')
    settings_path = load_settings()
    ls2_client = Locatieservices2Client(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # logging.debug('Debug functie zoek_puntlocatie_via_xy()')
    # x = 158190.21
    # y = 164766.15
    # wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_xy(x=x, y=y)
    # logging.debug(f'Gevonden Puntlocatie: {wegsegment_puntlocatie}')
    #
    # logging.debug('Debug functie zoek_puntlocatie_via_wegsegment()')
    # ident8='N0080001'
    # opschrift='4.6'
    # afstand='1.5'
    # wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_wegsegment(ident8=ident8, opschrift=opschrift, afstand=afstand)
    # logging.debug(f'Gevonden Puntlocatie: {wegsegment_puntlocatie}')

    # Import Excel-file as pandas dataframe
    filepath_excel = Path().home() / 'Downloads' / 'Lichtmast' / 'DA-2025-43571_export.xlsx'
    usecols = ['typeURI', 'assetId.identificator', 'naam', 'naampad', 'toestand', 'geometry']
    df_assets = pd.read_excel(filepath_excel, sheet_name='Lichtmast', usecols=usecols)

    for idx, df_asset in df_assets.iterrows():
        logging.debug(f'{idx}: Processing asset: {df_asset.get("assetId.identificator")[:36]}; naam: {df_asset.get("naam")}')
        asset = eminfra_client.get_asset_by_id(assettype_id=df_asset.get('assetId.identificator')[:36])
        logging.debug(f'{idx}: Processing asset: {asset.uuid}; naam: {asset.naam}')

        if not is_full_match(name=asset.naam, pattern='[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*\.[P]\d*'):
            logging.debug(f'{asset.type.label}: {asset.naam} volgt NIET de naamconventie')
            continue

        positie_rijweg, ident8_raw, opschrift = parse_lichtmast_naam(naam=asset.naam)

        wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_wegsegment(ident8=ident8_raw, opschrift=opschrift)
        wkt_geom_projected = coordinates_2_wkt(wegsegment_puntlocatie.geometry.coordinates)
        distance = get_euclidean_distance_wkt(wkt1=df_asset.get("geometry"), wkt2=wkt_geom_projected)

        # geometry_referentiepunt
