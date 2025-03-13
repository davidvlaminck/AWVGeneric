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
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)

    records=[]
    for record in fs_client.download_layer_to_records(layer='fietspaden_wrapp'):
        record = json.loads(record)
        record.update(record.pop('properties'))
        records.append(record)

    df = DataFrame(records)
    print(df.head())
    # df.to_excel('fietspaden.xlsx', index=False)


if __name__ == '__main__':
    cProfile.run('main()')
