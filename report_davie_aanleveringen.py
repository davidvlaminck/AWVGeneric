filters = {
    # "statusSubstatusCombinaties":
    #     [{"status": "IN_OPMAAK", "substatus": None},
    #      {"status": "DATA_AANGEVRAAGD", "substatus": "BESCHIKBAAR"},
    #      {"status": "DATA_AANGELEVERD", "substatus": "GOEDGEKEURD"}],
    # "creatieDatumVan": "2024-07-26",
    "verbergElisaAanleveringen": True
}

awv_acm_cookie = '55eb0c4eef5d41debd0d639c4db7e37c'
voId = '6c2b7c0a-11a9-443a-a96b-a1bec249c629'  # zie https://apps.mow.vlaanderen.be/eminfra/admin/gebruikers

import abc
from enum import Enum

from requests import Response, Session


class Environment(Enum):
    PRD = 'prd',
    DEV = 'dev',
    TEI = 'tei',
    AIM = 'aim'


class AuthType(Enum):
    JWT = 'JWT',
    CERT = 'cert',
    COOKIE = 'cookie'


class AbstractRequester(Session, metaclass=abc.ABCMeta):
    def __init__(self, first_part_url: str = ''):
        super().__init__()
        self.first_part_url = first_part_url

    @abc.abstractmethod
    def get(self, url: str = '', **kwargs) -> Response:
        return super().get(url=self.first_part_url + url, **kwargs)

    @abc.abstractmethod
    def post(self, url: str = '', **kwargs) -> Response:
        return super().post(url=self.first_part_url + url, **kwargs)

    @abc.abstractmethod
    def put(self, url: str = '', **kwargs) -> Response:
        return super().put(url=self.first_part_url + url, **kwargs)

    @abc.abstractmethod
    def patch(self, url: str = '', **kwargs) -> Response:
        return super().patch(url=self.first_part_url + url, **kwargs)

    @abc.abstractmethod
    def delete(self, url: str = '', **kwargs) -> Response:
        return super().delete(url=self.first_part_url + url, **kwargs)


class RequesterFactory:
    first_part_url_dict = {
        Environment.PRD: 'https://services.apps.mow.vlaanderen.be/',
        Environment.TEI: 'https://services.apps-tei.mow.vlaanderen.be/',
        Environment.DEV: 'https://services.apps-dev.mow.vlaanderen.be/',
        Environment.AIM: 'https://services-aim.apps-dev.mow.vlaanderen.be/'
    }

    @classmethod
    def create_requester(cls, auth_type: AuthType, env: Environment, settings: dict = None, **kwargs
                         ) -> AbstractRequester:

        try:
            first_part_url = cls.first_part_url_dict[env]
        except KeyError as exc:
            raise ValueError(f"Invalid environment: {env}") from exc

        if auth_type == AuthType.COOKIE:
            return CookieRequester(cookie=kwargs['cookie'], first_part_url=first_part_url.replace('services.', ''))
        else:
            raise ValueError(f"Invalid authentication type: {auth_type}")

class SNGatewayClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'sngateway/'

    def get_all_asset_filters(self) -> list[dict]:
        url = f'rest/eminfra/asset-filter'
        response = self.requester.get(url=url)
        return response.json()

    def add_new_asset_filter(self, uri: str, enabled: bool = True) -> None:
        url = f'rest/eminfra/asset-filter'
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


class DavieCoreClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'davie-aanlevering/api/'

    def aanlevering_by_id(self, id: str) -> dict:
        url = f'aanleveringen/{id}'
        response = self.requester.get(url=url)
        return response.json()

    def zoek_aanleveringen(self, filter_dict: dict) -> [dict]:
        _from = 0
        size = 100
        if filter_dict.get('sortBy') is None:
            filter_dict['sortBy'] = {"property": "creatieDatum", "order": "desc"}

        while True:
            url = f'aanleveringen/zoek?from={_from}&size={size}'
            response = self.requester.post(url=url, json=filter_dict)

            print(f'fetched up to {size} results from {url}')

            result_dict = response.json()
            yield from result_dict['data']

            if result_dict['links'].get('next') is None:
                break

            _from += size

    def historiek_by_aanlevering_id(self, id) -> [dict]:
        url = f'aanleveringen/{id}/historiek'
        response = self.requester.get(url=url)
        return response.json()


class TakenClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'takenservice/rest/awv-internal/taak/'

    def get_niet_afgesloten(self) -> [dict]:
        filter_dict = {"ascending": False, "statussen": ["BEZIG", "IN_WACHT", "UIT_TE_VOEREN"], "page": 0,
                       "pageSize": 1000, "voId": "6c2b7c0a-11a9-443a-a96b-a1bec249c629",
                       "typeKeys": ["aanlevering", "verificatie"], "metadata": [], "sortFieldNames": []}
        url = 'zoek'
        total = -1
        counted = 0
        while total == -1 or counted < total:
            response = self.requester.post(url=url, json=filter_dict)

            result_dict = response.json()
            total = result_dict['total']
            counted += filter_dict['pageSize']
            yield from result_dict['items']

            filter_dict['page'] += 1


class CookieRequester(AbstractRequester):
    def __init__(self, cookie: str = '', first_part_url: str = ''):
        super().__init__(first_part_url=first_part_url)
        self.cookie = cookie
        self.headers.update({'Cookie': f'acm-awv={cookie}'})

    def get(self, url: str = '', **kwargs) -> Response:
        return super().get(url=url, **kwargs)

    def post(self, url: str = '', **kwargs) -> Response:
        return super().post(url=url, **kwargs)

    def put(self, url: str = '', **kwargs) -> Response:
        return super().put(url=url, **kwargs)

    def patch(self, url: str = '', **kwargs) -> Response:
        return super().patch(url=url, **kwargs)

    def delete(self, url: str = '', **kwargs) -> Response:
        return super().delete(url=url, **kwargs)

if __name__ == '__main__':
    requester_davie = RequesterFactory.create_requester(
        cookie=awv_acm_cookie, auth_type=AuthType.COOKIE, env=Environment.TEI)
    sn_client = SNGatewayClient(requester=requester_davie)

    # sn_client.add_new_asset_filter(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Motorvangplank', enabled=True)

    json_data = sn_client.get_all_asset_filters()
    headers = ['id', 'version', 'created', 'updated', 'uri', 'enabled']
    rows = [[row.get(h, '') for h in headers] for row in sorted(json_data, key=lambda x: x['updated'], reverse=True)]

    from prettytable import PrettyTable
    table = PrettyTable(headers)
    table.add_rows(rows)
    print(table)