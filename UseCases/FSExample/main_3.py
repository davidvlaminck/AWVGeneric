import json
import logging
import time
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

    layers = [
    'innames', # 21 seconden
    "bebouwdekommen_wrapp", # 1 seconde
    "fietspaden_wrapp", # 13 seconden
    'uvroutes', # 4 seconden
    "referentiepunten2",  # 4 seconden
    ]

    for layer in layers:
        records=[]
        for record in fs_client.download_layer_to_records(layer=layer):
            record = json.loads(record)
            record.update(record.pop('properties'))
            records.append(record)

        df = DataFrame(records)
        print(df.head())
    # df.to_excel('fietspaden.xlsx', index=False)


if __name__ == '__main__':
    start = time.time()
    main()
    stop = time.time()
    print(f"Execution time: {round(stop - start, 2)}")
    # total 44.65
