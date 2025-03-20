import asyncio
import json
import time
from pathlib import Path

import aiohttp
from aiohttp import ClientSession
from pandas import DataFrame

from API.EMInfraClient import EMInfraClient
from API.Enums import Environment, AuthType
from API.FSClient import FSClient


async def coroutine_wrapper(async_gen):
    records=[]
    async for record in async_gen:
        try:
            record = json.loads(record)
            record.update(record.pop('properties'))
            records.append(record)
        except json.JSONDecodeError:
            print(f"Error: {record}")

    return DataFrame(records)


async def main():
    cookie='01fa3af312454ab1bce93c64d77d0a83'
    fs_client = FSClient(env=Environment.PRD, auth_type=AuthType.COOKIE, cookie=cookie)

    layers = [
        "bebouwdekommen_wrapp",  # 1 seconde # 33788
        'innames',  # 21 seconden 84659
        #
        "fietspaden_wrapp",  # 13 seconden 137170
        'uvroutes',  # 4 seconden 2470
        "referentiepunten2",  # 4 seconden 82616 records
    ]
    # total for 2k each = 4 seconds

    # tasks = []
    # connector = aiohttp.TCPConnector(limit=10)
    # async with ClientSession(connector=connector) as session:
    #     for layer in layers:
    #         tasks.append(asyncio.create_task(coroutine_wrapper(fs_client.download_layer_to_records2(
    #             layer=layer, session=session))))
    #
    #     results = await asyncio.gather(*tasks)
    #     for result in results:
    #         print(result.head())

    tasks = []
    connector = aiohttp.TCPConnector(limit=10)
    async with ClientSession(connector=connector) as session:
        for layer in layers:
            tasks.append(asyncio.create_task(coroutine_wrapper(fs_client.download_layer_to_records(
                layer=layer, session=session, start=0, page_size=1000))))
            tasks.append(asyncio.create_task(coroutine_wrapper(fs_client.download_layer_to_records(
                layer=layer, session=session, start=1000, page_size=1000))))
            tasks.append(asyncio.create_task(coroutine_wrapper(fs_client.download_layer_to_records(
                layer=layer, session=session, start=2000, page_size=1000))))

        results = await asyncio.gather(*tasks)
        for result in results:
            print(result.head())


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    stop = time.time()
    print(f"Execution time: {round(stop - start, 2)}")
    # total 44.65
    # 49.48
