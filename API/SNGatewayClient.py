from API.AbstractRequester import AbstractRequester


class SNGatewayClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'sngateway/'

    def get_all_asset_filters(self) -> list[dict]:
        url = 'rest/eminfra/asset-filter'
        response = self.requester.get(url=url)
        return response.json()

    def add_new_asset_filter(self, uri: str, enabled: bool = True) -> None:
        url = 'rest/eminfra/asset-filter'
        response = self.requester.post(url=url, json={
          "uri": uri,
          "enabled": enabled
        })
        return response.json()

    def modify_asset_filter(self, id: str, uri: str, enabled: bool = True) -> None:
        url = f'rest/eminfra/asset-filter/{id}'
        response = self.requester.put(url=url, json={
          "uri": uri,
          "enabled": enabled
        })
        return response.json()
