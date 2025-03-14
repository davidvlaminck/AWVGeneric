import asyncio
import json
import time
from pathlib import Path

from pandas import DataFrame

from API.EMInfraClient import EMInfraClient
from API.Enums import Environment, AuthType
from API.FSClient import FSClient


async def main():
    cookie='2eceed0bd1d44e50a507bb9e8d5eedfd'
    fs_client = FSClient(env=Environment.PRD, auth_type=AuthType.COOKIE, cookie=cookie)

    layers = [
        "bebouwdekommen_wrapp",  # 1 seconde
        'innames',  # 21 seconden

        "fietspaden_wrapp",  # 13 seconden
        'uvroutes',  # 4 seconden
        "referentiepunten2",  # 4 seconden
    ]

    for layer in layers:
        records = []
        async for record in fs_client.download_layer_to_records(layer=layer):
            record = json.loads(record)
            record.update(record.pop('properties'))
            records.append(record)

        df = DataFrame(records)
        print(df.head())
    # df.to_excel('fietspaden.xlsx', index=False)


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    stop = time.time()
    print(f"Execution time: {round(stop - start, 2)}")
    # total 44.65
    # 49.48
