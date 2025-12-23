import logging


class KenmerkService:
    def __init__(self, requester):
        self.requester = requester

    def get(self, asset_uuid: str, kenmerk_uuid: str) -> dict:
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerk_uuid}'
        resp = self.requester.get(url=url)
        if resp.status_code != 200:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())
        return resp.json()

    def put(self, asset_uuid: str, kenmerk_uuid: str, payload: dict) -> None:
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerk_uuid}'
        resp = self.requester.put(url=url, json=payload)
        if resp.status_code != 202:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())