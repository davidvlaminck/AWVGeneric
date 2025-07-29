import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

from API.Locatieservices2Client import Locatieservices2Client


def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Test Locatieservices2 (LS2)')
    settings_path = load_settings()
    ls2_client = Locatieservices2Client(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    print(ls2_client)
