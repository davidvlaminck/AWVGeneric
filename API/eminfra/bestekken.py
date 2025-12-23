import logging

from API.EMInfraDomain import BestekKoppeling


class BestekService:
    def __init__(self, requester):
        self.requester = requester
        self.BESTEKKOPPELING_UUID = 'ee2e627e-bb79-47aa-956a-ea167d20acbd'

    def get_by_asset_uuid(self, asset_uuid: str) -> [BestekKoppeling]:
        response = self.requester.get(
            url=f'core/api/installaties/{asset_uuid}/kenmerken/{self.BESTEKKOPPELING_UUID}/bestekken')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [BestekKoppeling.from_dict(item) for item in response.json()['data']]
