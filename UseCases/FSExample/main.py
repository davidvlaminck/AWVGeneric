import logging
from pathlib import Path

from API.Enums import AuthType, Environment
from API.FSClient import FSClient


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)

    response = fs_client.download_laag(laag='fietspaden_wrapp', file_path=Path('fietspaden.json'))