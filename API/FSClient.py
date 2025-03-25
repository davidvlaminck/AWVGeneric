from pathlib import Path
from typing import Iterator

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


    def download_layer_to_records(self, layer: str, chunk_size: int = 1024*256):
        response = self.requester.get(url=f'{layer}/query?fmt=json&projection=properties', stream=True)
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        chunk_rest = ''

        with tqdm(unit=' records', desc=layer) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    chunk_rest += chunk.decode("utf-8")
                    chunk_rest = yield from self._process_chunk(chunk_rest, pbar)
                elif chunk_rest:
                    chunk_rest = yield from self._process_chunk(chunk_rest, pbar)

        print(f"\r✅ {pbar.n} records gedownload.")

    @classmethod
    def _process_chunk(cls, chunk_rest, pbar) -> str:
        chunks = chunk_rest.split('\n')
        chunk_rest = chunks.pop(-1)
        pbar.update(len(chunks))
        yield from chunks
        return chunk_rest
