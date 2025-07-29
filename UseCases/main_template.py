import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def read_report():
    """Read RSA-report as input into a DataFrame."""
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / '[RSA] Wetenschappelijke notaties komen niet voor.xlsx'
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "attribuutnaam"])
    return df_assets

def functie() -> None:
    """"""
    return None

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Use case naam:\t use case beschrijving')
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = read_report()

    for _, asset in df_assets.iterrows():
        functie()