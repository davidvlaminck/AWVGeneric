import abc

import aiohttp
from aiohttp import ClientSession, ClientResponse
from aiohttp.typedefs import StrOrURL
from requests import Session, Response


class AbstractRequester(metaclass=abc.ABCMeta):
    def __init__(self, first_part_url: str = '', retries: int = 3):
        self.first_part_url = first_part_url
        self.headers = {}
        if retries < 1:
            raise ValueError("retries must be at least 1")
        self.retries = retries

    async def get_async(self, url = '', **kwargs) -> (dict, str):
        async with ClientSession() as session:
            async with session.get(url=self.first_part_url + url, headers=self.headers, **kwargs) as response:
                for _ in range(self.retries):
                    if str(response.status).startswith('2'):
                        headers = dict(response.headers)
                        content = await response.text()

                        return headers, content
        raise RuntimeError(f"GET request failed after {self.retries} retries. Last response: {await response.text()}")

    def post(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().post(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        raise RuntimeError(f"POST request failed after {self.retries} retries. Last response: {response}")

    def put(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().put(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        raise RuntimeError(f"PUT request failed after {self.retries} retries. Last response: {response}")

    def patch(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().patch(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        raise RuntimeError(f"PATCH request failed after {self.retries} retries. Last response: {response}")

    def delete(self, url: str = '', **kwargs) -> Response:
        response = None
        for _ in range(self.retries):
            response = super().delete(url=self.first_part_url + url, **kwargs)
            if str(response.status_code).startswith('2'):
                return response
        raise RuntimeError(f"DELETE request failed after {self.retries} retries. Last response: {response}")
