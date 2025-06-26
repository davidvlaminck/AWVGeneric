import json
import logging
from pathlib import Path

from pandas import DataFrame

from API.Enums import AuthType, Environment
from API.FSClient import FSClient
from utils.decorators import print_timing


@print_timing
def download_records_from_fs() -> None:
    settings_path = Path('C:\\Users\\vlaminda\\Documents\\resources\\settings_SyncOTLDataToLegacy.json')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)
    return fs_client.download_layer_to_records(layer='fietspaden_wrapp')

@print_timing
def from_records_to_df(record_generator) -> DataFrame:
    records = []
    for record in record_generator:
        record = json.loads(record)
        record.update(record.pop('properties'))
        records.append(record)

    return DataFrame(records)

@print_timing
def main():
    logging.basicConfig(level=logging.INFO)

    records_generator = download_records_from_fs()
    return from_records_to_df(records_generator)


if __name__ == '__main__':
    df = main()
    print(df.info(verbose=True))
