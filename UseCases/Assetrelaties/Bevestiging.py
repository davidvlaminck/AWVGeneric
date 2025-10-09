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
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Aanmaken van een Bevestiging-Relatie tussen 2 Legacy assets.'
                 'Opdracht om Kasten, LS, LSDeel, Afstandsbewaking en Segmentcontrollers te localiseren.')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    KENMERKTYPE_UUID, RELATIETYPE_UUID = eminfra_client.get_kenmerktype_and_relatietype_id(relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')

    sheets = ['kast_ab', 'kast_ls', 'kast_lsdeel', 'lsdeel_segc']

    for sheet in sheets:
        df_assets = read_excel_as_dataframe(
            filepath=get_filepath(), sheet_name=sheet, usecols=["bron_uuid", "bron_naam", "doel_uuid", "doel_naam"])

        # Filter out assets with existing relationships
        df_assets = df_assets[df_assets.apply(lambda asset: next(eminfra_client.search_relaties(assetId=asset["bron_uuid"], kenmerkTypeId=KENMERKTYPE_UUID, relatieTypeId=RELATIETYPE_UUID), None) is None, axis=1)]

        for idx, asset in df_assets.iterrows():
            bevestigingrelatie_uuid = eminfra_client.create_assetrelatie(
                bronAsset_uuid=asset.get("bron_uuid")
                , doelAsset_uuid=asset.get("doel_uuid")
                , relatieType_uuid=RELATIETYPE_UUID
            )
            logging.info(f'Bevestiging-relatie ({bevestigingrelatie_uuid}) aangemaakt tussen bron-asset {asset["bron_uuid"]}: {asset["bron_naam"]} en doel-asset {asset["doel_uuid"]}: {asset["doel_naam"]}')