import base64
import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, LocatieKenmerk
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Classes.Onderdeel.Netwerkelement import Netwerkelement
from otlmow_converter.OtlmowConverter import OtlmowConverter

logging.info("""
        Update de locatie van Netwerkelement (OTL)
        Selecteer Netwerkelement waar de eigenschap gebruik niet gelijk is aan "CEN"; "OTN"; "SDH"
        Neem de locatie van het asset waarmee het Netwerkelement verbonden is (via HoortBij-relatie).
        Indien de verbonden asset geen locatie heeft, dan doe niets.
        
        Voorbeeld: 
        https://apps.mow.vlaanderen.be/eminfra/assets/144cc6a9-7668-4e53-b76d-61247c86654d 
      """)

def load_settings():
    """Load API settings from JSON"""
    return (
        Path().home()
        / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    )

def read_report():
    """Read RSA-report as input into a DataFrame."""
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / 'locatie' / '[RSA] IP Netwerkelementen hebben dezelfde locatie als hun Bijhorende legacy object van type IP.xlsx'
    return pd.read_excel(
        filepath,
        sheet_name='Resultaat',
        header=2,
        usecols=[
            "uuid_netwerkelement",
            "netwerkelement_naam",
            "netwerkelement_geometry",
            "uuid_ip",
            "ip_naam",
            "ip_naampad",
            "ip_geometry",
            "afstand",
        ],
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
    # tot hier
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    settings_path = load_settings()
    # excel_path = Path().home() / 'Downloads' / 'update_eigenschap' / 'locatie'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # df_report = read_report()
    eminfra_client.search_assets(query_dto=query_dto, actief=True)
    # update_assets = []
    # for _, asset in df_report.iterrows():
    #     print(f"Updating eigenschap 'locatie' for asset Netwerkelement: {asset.uuid_netwerkelement}")
    #
    #     ################################################################################
    #     ###  Get eigenschap locatie
    #     ################################################################################
    #     # Eigenschap locatie ophalen via API-call
    #     locatie_actueel = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid_netwerkelement)
    #     locatie_actueel_wkt = locatie_actueel.geometrie # OTL
    #
    #     # Eigenschap locatie ophalen via API-call
    #     locatie_nieuw = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid_ip)
    #     locatie_nieuw_wkt = format_locatie_kenmerk_lgc_2_wkt(locatie_nieuw)
    #
    #     #################################################################################
    #     ####  Create asset, set geometry
    #     #################################################################################
    #     netwerkelement = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.uuid_netwerkelement), None)
    #     # instantiate object netwerkelement
    #     new_asset = Netwerkelement()
    #     encoded_suffix = base64.b64encode(new_asset.typeURI.rsplit('/', 1)[-1].encode()).decode()
    #     new_asset.assetId.identificator = netwerkelement.uuid + '-' + encoded_suffix
    #     new_asset.assetId.toegekendDoor = 'AWV'
    #     new_asset.geometry = locatie_nieuw_wkt
    #
    #     update_assets.append(new_asset)
    #
    # #################################################################################
    # ####  Write to DAVIE-compliant file
    # #################################################################################
    # if update_assets:
    #     file_path = excel_path / 'Netwerkelementen_update_locatie.xlsx'
    #     print(f'Write DAVIE file to: {file_path}')
    #     OtlmowConverter.from_objects_to_file(
    #         file_path=file_path,
    #         sequence_of_objects=update_assets)