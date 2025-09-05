import logging
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

def read_excel_as_dataframe(filepath: Path, sheet_name: str, usecols: list[str]):
    """Read RSA-report as input into a DataFrame."""
    return pd.read_excel(filepath, sheet_name=sheet_name, header=0, usecols=usecols).dropna(subset=usecols)

def get_filepath():
    return Path().home() / 'Downloads' / 'SegmentController_Afstandsbewaking' / 'Bevestigingsrelaties.xlsx'


if __name__ == '__main__':
    logging.basicConfig(filename="../../Assetrelaties/logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Updaten van de locatie-eigenschap van absoluut naar afgeleid.')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    sheets = ['kast_ab', 'kast_ls', 'kast_lsdeel', 'lsdeel_segc']

    for sheet in sheets:
        df_assets = read_excel_as_dataframe(
            filepath=get_filepath(), sheet_name=sheet, usecols=["bron_uuid", "bron_naam", "doel_uuid", "doel_naam"])

        # Filter out assets with existing "afgeleide locaties"
        df_assets = df_assets[df_assets.apply(lambda asset: eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset["bron_uuid"]).locatie is None, axis=1)]

        for idx, asset in df_assets.iterrows():
            logging.info(f'Locatie eigenschap updaten tussen bron-asset {asset["bron_uuid"]}: {asset["bron_naam"]} en doel-asset {asset["doel_uuid"]}: {asset["doel_naam"]}')
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset["bron_uuid"], doel_asset_uuid=asset["doel_uuid"])