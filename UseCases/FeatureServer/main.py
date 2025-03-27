import json
import logging
from pathlib import Path

from pandas import DataFrame

from API.Enums import AuthType, Environment
from API.FSClient import FSClient


import cProfile

def main():
    logging.basicConfig(level=logging.INFO)

    settings_path = Path('C:\\Users\\vlaminda\\Documents\\resources\\settings_SyncOTLDataToLegacy.json')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)

    file_path = Path('tunnels.json')
    response = fs_client.download_layer(layer='fietspaden_wrapp', file_path=file_path)

    dict_list = []
    with open(file_path, 'r', encoding='utf-8') as file:
        keys = []
        for line in file:
            data = json.loads(line)
            data.update(data.pop('properties'))
            dict_list.append(data)

    df = DataFrame(dict_list)
    print(df.head())
    df.to_excel('fietspaden.xlsx', index=False)


if __name__ == '__main__':
    cProfile.run('main()')
