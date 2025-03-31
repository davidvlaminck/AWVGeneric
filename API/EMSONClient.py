from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

from API.EMInfraDomain import BaseDataclass
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory

@dataclass
class Query(BaseDataclass):
    size: int
    filters: dict
    orderByProperty: str
    fromCursor: str | None = None


class EMSONClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'emson/'

    def get_asset_by_uuid(self, uuid: str) -> dict:
        response = self.requester.get(url=f'api/otl/assets/{uuid}')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def get_assets(self) -> Generator[dict]:
        paging_cursor = None
        while True:
            response = self.requester.get( url='api/otl/assets', headers={'em-paging-cursor': paging_cursor})
            if response.status_code != 200:
                print(response)
                raise ProcessLookupError(response.content.decode("utf-8"))
            print('fetched 100 results')
            yield from response.json()['@graph']
            paging_cursor = response.headers.get('em-paging-next-cursor')
            if paging_cursor is None:
                break

    def get_assetrelatie_by_uuid(self, uuid: str) -> dict:
        response = self.requester.get(url=f'api/otl/assetrelaties/{uuid}')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def get_assets_by_filter(self, filter: dict, size: int = 100, order_by_property: str = None) -> [dict]:
        """See https://apps.mow.vlaanderen.be/emson/docs/#_post_emsonapiotlassetssearch for more details
        +---------------------+----------------------------------------+------------------------------------+\n
        |       Filter        |              Omschrijving              |                Type                |\n
        +---------------------+----------------------------------------+------------------------------------+\n
        | uuid                | uuid van asset                         | string of string[]                 |\n
        | id                  | uuid van asset                         | string of string[]                 |\n
        | aimId               | aim-id van asset                       | string of string[]                 |\n
        | naam                | naam van asset                         | string                             |\n
        | actief              | actief of niet?                        | "true" of "false"                  |\n
        | typeUri             | uri van het type van de asset          | string of string[]                 |\n
        | typeUuid            | uuid van het type van de asset         | string of string[]                 |\n
        | intersect           | intersect met de geometry van de asset | wkt string                         |\n
        | attributen          | alle (otl) attributen                  | zie Filter op attributen           |\n
        | heeftBetrokkene     | betrokkene relaties                    | zie Filter op betrokkene relaties  |\n
        | bestekKoppeling     | gekoppelde bestekken                   | zie Filter op gekoppelde bestekken |\n
        | aangemaaktInContext | asset aangemaakt in context            | string of string[]                 |\n
        | gewijzigdInContext  | asset gewijzigd in context             | string of string[]                 |\n
        +---------------------+----------------------------------------+------------------------------------+
        """
        query = Query(filters=filter, size=size, orderByProperty=order_by_property)
        while True:
            response = self.requester.post(url='api/otl/assets/search', data=query.json())
            if response.status_code != 200:
                print(response)
                raise ProcessLookupError(response.content.decode("utf-8"))
            yield from response.json()['@graph']
            paging_cursor = response.headers.get('em-paging-next-cursor')
            if paging_cursor is None:
                break
            query.fromCursor = paging_cursor

    def get_assetrelaties_by_filter(self, filter: dict, size: int = 100, order_by_property: str = None) -> [dict]:
        """
        +-----------+---------------------------------------------------------------------------------+--------------------+\n
        |  Filter   |                                  Omschrijving                                   |        Type        |\n
        +-----------+---------------------------------------------------------------------------------+--------------------+\n
        | uuid      | Lijst van relatie uuid’s                                                        | string[]           |\n
        | aimId     | Lijst van relatie aimId’s                                                       | string[]           |\n
        | bronAsset | Asset uuid of lijst van asset uuid’s, van assets die als bron ofvoorkomen       | string of string[] |\n
        | doelAsset | Asset uuid of lijst van asset uuid’s, van assets die als doel voorkomen         | string of string[] |\n
        | asset     | Asset uuid of lijst van asset uuid’s, van assets die als bron of doel voorkomen | string of string[] |\n
        +-----------+---------------------------------------------------------------------------------+--------------------+
        """
        query = Query(filters=filter, size=size, orderByProperty=order_by_property)
        while True:
            response = self.requester.post(url='api/otl/assetrelaties/search', data=query.json())
            if response.status_code != 200:
                print(response)
                raise ProcessLookupError(response.content.decode("utf-8"))

            yield from response.json()['@graph']
            paging_cursor = response.headers.get('em-paging-next-cursor')
            if paging_cursor is None:
                break
            query.fromCursor = paging_cursor
