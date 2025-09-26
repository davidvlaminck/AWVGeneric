import logging
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

SETTINGS_PATH = Path.home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
INPUT_FILE = Path.home() / 'Downloads/SegmentController_Afstandsbewaking/Bevestigingsrelaties.xlsx'
SHEETS = ['Bevestiging']

def read_excel_as_dataframe(filepath: Path, sheet_name: str, usecols: list[str]):
    """Read RSA-report as input into a DataFrame."""
    return pd.read_excel(filepath, sheet_name=sheet_name, header=0, usecols=usecols).dropna(subset=usecols)

if __name__ == '__main__':
    logging.basicConfig(filename='logs.log', level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Updaten van de locatie-eigenschap van absoluut naar afgeleid.')
    logging.info('Input file is op basis van een volledige DAVIE download van SegmentControllers en Afstandsbewaking, inclusief relaties.')

    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=SETTINGS_PATH)

    for sheet in SHEETS:
        df_assets = read_excel_as_dataframe(
            filepath=INPUT_FILE, sheet_name=sheet, usecols=["bronAssetId.identificator", "bron.typeURI", "doelAssetId.identificator", "doel.typeURI"])

        # Filter out assets with existing "afgeleide locaties"
        df_assets = df_assets[df_assets.apply(lambda asset: eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset["bronAssetId.identificator"][:36]).locatie is None, axis=1)]

        for idx, asset in df_assets.iterrows():
            logging.info(f'Locatie eigenschap updaten tussen bron-asset {asset["bronAssetId.identificator"][:36]}: {asset["bron.typeURI"]} en doel-asset {asset["doelAssetId.identificator"][:36]}: {asset["doel.typeURI"]}')
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset["bronAssetId.identificator"][:36], doel_asset_uuid=asset["doelAssetId.identificator"][:36])