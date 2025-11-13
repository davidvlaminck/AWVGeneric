import base64
import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import LocatieKenmerk
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Classes.Onderdeel.Lichtmast import Lichtmast
from otlmow_converter.OtlmowConverter import OtlmowConverter


def load_settings():
    """Load API settings from JSON"""
    return (
        Path().home()
        / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    )

def read_report(filepath: Path, usecols=list[str]):
    """Read RSA-report as input into a DataFrame."""
    return pd.read_excel(
        filepath,
        sheet_name='Resultaat',
        header=2,
        usecols=usecols
    )

def format_locatie_kenmerk_lgc_2_wkt(locatie: LocatieKenmerk) -> str:
    """
    Format LocatieKenmerk as input to a WKT string as output
    Supported geometry formats: Point
    :param locatie: LocatieKenmerk 
    :return: 
    """
    if locatie.locatie.get('geometrie') is None:
        return None
    if locatie.locatie.get('_type') != 'punt':
        # implementation for other geometry types
        return None
    coordinaten = locatie.locatie.get('coordinaten')
    return f'POINT Z ({coordinaten.get("x")} {coordinaten.get("y")} {coordinaten.get("z", 0)})'



if __name__ == '__main__':
    settings_path = load_settings()
    excel_path = Path().home() / 'Downloads' / 'Lichtmast'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info("""
        Update de eigenschap locatie van asset Lichtmast (OTL)
        Lichtmast (OTL) krijgt de locatie van de Kast uit dezelfde boomstructuur. 
    """)

    df_assets = read_report(filepath=excel_path/'[RSA] Lichtmast heeft een locatie.xlsx', usecols=["uuid", "naam", "naampad"])

    update_assets = []

    for idx, row_asset in df_assets.iterrows():
        asset_uuid = row_asset.get("uuid")
        logging.info(f'Processing {idx} asset: {asset_uuid}')

        asset = eminfra_client.get_asset_by_id(asset_id=asset_uuid)

        ################################################################################
        ###  Get eigenschap locatie
        ################################################################################
        # Eigenschap locatie ophalen via API-call
        locatie_actueel = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)
        locatie_actueel_wkt = locatie_actueel.geometrie # OTL

        installatie = eminfra_client.search_parent_asset(asset_uuid=asset.uuid, return_all_parents=False, recursive=True)
        child_assets = eminfra_client.search_child_assets(asset_uuid=installatie.uuid, recursive=True)

        kast_assets = [item for item in child_assets if item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#Kast']
        if len(kast_assets) != 1:
            raise ValueError(f'Er dient exact 1 kast in de boomstructuur te staan van beheerobject: {installatie.uuid}')
        else:
            kast_asset = kast_assets[0]

        # Eigenschap locatie ophalen via API-call
        kast_locatie = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=kast_asset.uuid)
        kast_locatie_wkt = format_locatie_kenmerk_lgc_2_wkt(kast_locatie)

        #################################################################################
        ####  Create asset, set geometry
        #################################################################################
        # instantiate object Lichtmast
        new_asset = Lichtmast()
        encoded_suffix = base64.b64encode(new_asset.typeURI.rsplit('/', 1)[-1].encode()).decode()
        new_asset.assetId.identificator = asset.uuid + '-' + encoded_suffix
        new_asset.assetId.toegekendDoor = 'AWV'
        new_asset.geometry = kast_locatie_wkt

        update_assets.append(new_asset)

    #################################################################################
    ####  Write to DAVIE-compliant file
    #################################################################################
    if update_assets:
        file_path = excel_path / 'DA-2025-XXXXX_Lichtmast_update_locatie.xlsx'
        print(f'Write DAVIE file to: {file_path}')
        OtlmowConverter.from_objects_to_file(
            file_path=file_path,
            sequence_of_objects=update_assets)