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
    opschrift = str(round(float(lichtmast_naam_basis[index+1:]), 1)) # afronden tot op 1 decimaal getal


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
    filepath_excel_input = Path().home() / 'Downloads' / 'Lichtmast' / 'DA-2025-43571_export.xlsx'
    filepath_excel_output = Path().home() / 'Downloads' / 'Lichtmast' / 'DA-2025-XXXXX_import.xlsx'
    usecols = ['typeURI', 'assetId.identificator', 'naam', 'naampad', 'toestand', 'geometry']
    df_assets = pd.read_excel(filepath_excel_input, sheet_name='Lichtmast', usecols=usecols)

    geometrie_refentiepunt = []
    afstand = []
    for idx, df_asset in df_assets.iterrows():
        logging.debug(f'{idx}: Processing asset: {df_asset.get("assetId.identificator")[:36]}; naam: {df_asset.get("naam")}')
        asset = eminfra_client.get_asset_by_id(assettype_id=df_asset.get('assetId.identificator')[:36])
        logging.debug(f'{idx}: Processing asset: {asset.uuid}; naam: {asset.naam}')
        if asset.naam is None:
            geometrie_refentiepunt.append(None)
            afstand.append(None)
            continue

        if not is_full_match(name=asset.naam, pattern='[a-zA-Z]{1}\d{1,3}[NPMXnpmx]{1}\d+\.?\d*\.[P]\d*'):
            logging.debug(f'{asset.type.label}: {asset.naam} volgt NIET de naamconventie')
            geometrie_refentiepunt.append(None)
            afstand.append(None)
            continue

        positie_rijweg, ident8, opschrift = parse_lichtmast_naam(naam=asset.naam)

        logging.debug('Zoek puntlocatie via wegsegment.')
        logging.debug(f'ident8: {ident8}')
        logging.debug(f'opschrift: {opschrift}')

        try:
            wegsegment_puntlocatie = ls2_client.zoek_puntlocatie_via_wegsegment(ident8=ident8, opschrift=opschrift)
            wkt_geom_referentiepunt = coordinates_2_wkt(wegsegment_puntlocatie.geometry.coordinates)

            wkt1 = df_asset.get("geometry")
            wkt2 = wkt_geom_referentiepunt
            if pd.isna(wkt1) or pd.isna(wkt2):
                distance = None
            else:
                distance = round(get_euclidean_distance_wkt(wkt1=df_asset.get("geometry"), wkt2=wkt_geom_referentiepunt))

        except:
            logging.debug('Locatieservices kon de locatie niet ophalen.')
            wkt_geom_referentiepunt = 'werd niet teruggevonden'
            distance = None

        geometrie_refentiepunt.append(wkt_geom_referentiepunt)
        afstand.append(distance)


    # Append to dataframe
    df_assets["geometrie_referentiepunt"] = geometrie_refentiepunt
    df_assets["afstand"] = afstand

    # Append hyperlink
    df_assets["eminfra"] = 'https://apps.mow.vlaanderen.be/eminfra/assets/' + df_assets["assetId.identificator"][:36]

    logging.debug(f'Write pandas dataframe to Excel: {filepath_excel_output}')
    df_assets.to_excel(filepath_excel_output, sheet_name='Lichtmast', freeze_panes=[1,2], index=False)
