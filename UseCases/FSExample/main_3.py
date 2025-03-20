import json
import logging
import time
from pathlib import Path

from pandas import DataFrame

from API.Enums import AuthType, Environment
from API.FSClient import FSClient
import cProfile


def main(chunk_size: int):
    logging.basicConfig(level=logging.INFO)

    settings_path = Path('C:\\Users\\vlaminda\\Documents\\resources\\settings_SyncOTLDataToLegacy.json')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)

    layers = [
    #'innames', # 21 seconden
    "bebouwdekommen_wrapp", # 1 seconde
    #"fietspaden_wrapp", # 13 seconden
    'uvroutes', # 4 seconden
    "referentiepunten2",  # 4 seconden
    ]

    for layer in layers:
        records=[]
        for record in fs_client.download_layer_to_records(layer=layer, chunk_size=chunk_size):
            record = json.loads(record)
            record.update(record.pop('properties'))
            records.append(record)

        df = DataFrame(records)
        print(df.head())
    # df.to_excel('fietspaden.xlsx', index=False)


if __name__ == '__main__':
    for chunk_size in [1024*256]:
        start = time.time()
        main(chunk_size=chunk_size)
        stop = time.time()
        print(f"Execution time with chunk_size {chunk_size}: {round(stop - start, 2)}")

    # 16kB : 19.12 - 15.72
    # 32kB : 23.04 - 19.29
    # 64kB : 20.11 - 18.96
    # 128kB : 13.17 - 27.63
    # 256kB : 17.21 - 12.69
    # 512kB : 20.23 - 17.7
    # 768kB : 29.48 - 16.63
    # 1024kB : 31.31 - 18.08
    # 2048kB : 24.53 - 19.73
    # 5MB  28.201
    # 10MB  25.485