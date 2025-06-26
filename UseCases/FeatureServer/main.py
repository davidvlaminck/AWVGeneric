import json
import logging
from pathlib import Path

from pandas import DataFrame

from API.Enums import AuthType, Environment
from API.FSClient import FSClient
from utils.decorators import print_timing


@print_timing
def download_file_from_fs(file_path: Path) -> None:
    settings_path = Path('C:\\Users\\vlaminda\\Documents\\resources\\settings_SyncOTLDataToLegacy.json')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)
    fs_client.download_layer(layer='fietspaden_wrapp', file_path=file_path)

@print_timing
def from_file_to_df_manual_read(file_path) -> DataFrame:
    dict_list = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            data.update(data.pop('properties'))
            dict_list.append(data)
    return DataFrame(dict_list)

@print_timing
def main():
    logging.basicConfig(level=logging.INFO)
    file_path = Path('fietspaden_wrapp.json')

    download_file_from_fs(file_path)

    return from_file_to_df_manual_read(file_path)


if __name__ == '__main__':
    df = main()
    print(df.info(verbose=True))

