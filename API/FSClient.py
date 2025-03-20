from pathlib import Path

from aiohttp import ClientSession
from tqdm import tqdm

from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


class FSClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'geolatte-nosqlfs/cert/api/databases/featureserver/'

    def download_layer(self, layer: str, file_path: Path) -> None:
        response = self.requester.get(url=f'{layer}/query?fmt=json&projection=properties', stream=True)
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        total_size = 0
        chunk_size = 1024 * 1024  # 1 MB

        with open(file_path, 'wb') as f, tqdm(unit_scale=True, unit='B', desc=file_path.name) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
                    pbar.update(len(chunk))

        print(f"\r✅ {pbar.n / (1000*1000)} MB gedownload.")

    @classmethod
    async def read_all_lines(cls, stream_reader):
        line = await stream_reader.read()
        return line.decode('utf-8').strip()

    async def download_layer_to_records(self, layer: str, session, page_size: int = 1000):
        start = 0
        for _ in range(self.requester.retries):
            with tqdm(unit=' records', desc=layer) as pbar:
                while True:
                    url = f'{self.requester.first_part_url}{layer}/query?fmt=json&projection=properties&start={start}&limit={page_size}'
                    url = url.replace('services.', '').replace('/cert', '')
                    async with session.get(url=url, headers=self.requester.headers) as response:
                        try:
                            if not str(response.status).startswith('2'):
                                raise RuntimeError(f"Error: {response.text}")

                            decoded = await self.read_all_lines(response)
                            chunks = decoded.split('\n')
                            if chunks[-1] == '':
                                chunks.pop(-1)
                            if not chunks:
                                return
                            start += len(chunks)
                            pbar.update(len(chunks))

                            for c in chunks:
                                yield c

                            # print(f"\r✅ {pbar.n} records gedownload.")
                        except Exception as ex:
                            print(ex)

        raise RuntimeError(f"GET request failed after {self.requester.retries} retries. Last response:"
                           f" {response.text}")


    async def download_layer_to_records2(self, layer: str, session):
        total_size = 0
        chunk_size = 1024 * 1024  # 1 MB
        chunk_rest = ''

        url = f'{self.requester.first_part_url}{layer}/query?fmt=json&projection=properties'
        url = url.replace('services.', '').replace('/cert', '')

        for _ in range(self.requester.retries):
            async with session.get(url=url, headers=self.requester.headers) as response:
                try:
                    with tqdm(unit=' records', desc=layer) as pbar:
                        if str(response.status).startswith('2'):
                            async for chunk in response.content.iter_chunked(chunk_size):
                                if chunk:
                                    chunk_rest += chunk.decode("utf-8")
                                    chunks = chunk_rest.split('\n')
                                    chunk_rest = chunks.pop(-1)
                                    total_size += len(chunks)
                                    pbar.update(len(chunks))
                                    for c in chunks:
                                        yield c

                                else:
                                    if chunk_rest:
                                        total_size += 1
                                        pbar.update(1)
                                        yield chunk_rest
                            return
                    print(f"\r✅ {pbar.n} records gedownload.")
                except Exception as ex:
                    print(ex)

        raise RuntimeError(f"GET request failed after {self.requester.retries} retries. Last response:"
                           f" {response.text}")




