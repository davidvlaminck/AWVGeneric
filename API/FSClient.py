from pathlib import Path

from pathlib import Path
from tqdm import tqdm

from API.EMInfraDomain import BestekKoppeling
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


class FSClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'geolatte-nosqlfs/cert/api/databases/featureserver/'

    def download_laag(self, laag: str, file_path: Path) -> None:
        response = self.requester.get(
            url=f'{laag}/query?fmt=json&projection=properties', stream=True)
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
