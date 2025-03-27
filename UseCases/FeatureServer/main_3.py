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
        #'innames',
        # "bebouwdekommen_wrapp",
        # "fietspaden_wrapp",
        'uvroutes',
        #"referentiepunten2",
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
