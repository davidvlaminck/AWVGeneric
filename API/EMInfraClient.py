import json
from collections.abc import Generator
from pathlib import Path

from API.EMInfraDomain import OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, PagingModeEnum, QueryDTO, BestekRef, \
    BestekKoppeling, FeedPage, AssettypeDTO, AssettypeDTOList, DTOList, AssetDTO, BetrokkenerelatieDTO, AgentDTO
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

    def get_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str) -> [BestekKoppeling]:
        response = self.requester.get(
            url=f'core/api/installaties/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        print(response.json()['data'])

        return [BestekKoppeling.from_dict(item) for item in response.json()['data']]

    def get_bestekref_by_eDelta_dossiernummer(self, eDelta_dossiernummer: str) -> [BestekRef]:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaDossiernummer',
                                                    operator=OperatorEnum.EQ,
                                                    value=eDelta_dossiernummer)])]))

        response = self.requester.post('core/api/bestekrefs/search', data=query_dto.json())
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        print(response.json()['data'])

        return [BestekRef.from_dict(item) for item in response.json()['data']]

    def get_feedproxy_page(self, feed_name: str, page_num: int, page_size: int = 1):
        url = f"feedproxy/feed/{feed_name}/{page_num}/{page_size}"
        json_dict = self.requester.get(url).json()
        return FeedPage.from_dict(json_dict)

    def get_assettype_by_id(self, assettype_id: str) -> AssettypeDTO:
        url = f"core/api/assettypes/{assettype_id}"
        json_dict = self.requester.get(url).json()
        return AssettypeDTO.from_dict(json_dict)

    def get_all_assettypes(self, size: int = 100) -> Generator[AssettypeDTO]:
        from_ = 0
        while True:
            url = f"core/api/assettypes?from={from_}&size={size}"
            json_dict = self.requester.get(url).json()
            yield from [AssettypeDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            from_ = json_dict['from'] + size
            if from_ >= dto_list_total:
                break

    def get_all_legacy_assettypes(self, size: int = 100) -> Generator[AssettypeDTO]:
        yield from [assettype_dto for assettype_dto in self.get_all_assettypes(size)
                    if assettype_dto.korteUri.startswith('lgc:')]

    def get_all_otl_assettypes(self, size: int = 100) -> Generator[AssettypeDTO]:
        yield from [assettype_dto for assettype_dto in self.get_all_assettypes(size)
                    if ':' not in assettype_dto.korteUri]

    def get_asset_by_id(self, assettype_id: str) -> AssetDTO:
        url = f"core/api/assets/{assettype_id}"
        json_dict = self.requester.get(url).json()
        return AssetDTO.from_dict(json_dict)

    def search_assets(self, query_dto: QueryDTO) -> Generator[AssetDTO]:
        query_dto.from_ = 0
        if query_dto.size is None:
            query_dto.size = 100
        url = "core/api/assets/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [AssetDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_betrokkenerelaties(self, query_dto: QueryDTO) -> Generator[BetrokkenerelatieDTO]:
        query_dto.from_ = 0
        if query_dto.size is None:
            query_dto.size = 100
        url = "core/api/betrokkenerelaties/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [BetrokkenerelatieDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break


    def get_objects_from_oslo_search_endpoint(self, url_part: str,
                                              filter_string: dict = '{}', size: int = 100,
                                              expansions_fields: [str] = None) -> Generator:
        """Returns Generator objects for each OSLO endpoint

        :param url_part: keyword to complete the url
        :type url_part: str
        :param filter_string: filter condition
        :type filter_string: dict
        :param size: amount of objects to return in 1 page or request
        :type size: int
        :param expansions_fields: additional fields to append to the results
        :type expansions_fields: [str]
        :return: Generator
        """
        body = {'size': size, 'fromCursor': None, 'filters': filter_string}
        if expansions_fields:
            body['expansion']['fields'] = expansions_fields
        paging_cursor = None
        url = f'core/api/otl/{url_part}/search'

        while True:
            # update fromCursor
            if paging_cursor:
                body['fromCursor'] = paging_cursor
            json_body = json.dumps(body)

            response = self.requester.post(url=url, data=json_body)
            decoded_string = response.content.decode("utf-8")
            dict_obj = json.loads(decoded_string)

            yield from dict_obj["@graph"]

            if 'em-paging-next-cursor' in response.headers.keys():
                paging_cursor = response.headers['em-paging-next-cursor']
            else:
                break

    def search_agent(self, query_dto: QueryDTO) -> Generator[AgentDTO]:
        query_dto.from_ = 0
        if query_dto.size is None:
            query_dto.size = 100
        url = "core/api/agents/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [AgentDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def add_betrokkenerelatie(self, asset_uuid: str, agent_uuid: str, rol: str = 'toezichter') -> dict:
        json_body = {
            "bron": {
                "uuid": f"{asset_uuid}"
                , "_type": "onderdeel"
            },
            "doel": {
                "uuid": f"{agent_uuid}"
                , "_type": "agent"
            },
            "rol": f"{rol}"
        }
        response = self.requester.post(url='core/api/betrokkenerelaties', json=json_body)
        if response.status_code != 202:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def remove_betrokkenerelatie(self, betrokkenerelatie_uuid: str) -> dict:
        url = f"core/api/betrokkenerelaties/{betrokkenerelatie_uuid}"
        response = self.requester.delete(url=url)
        if response.status_code != 202:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response


    def get_oef_schema_as_json(self, name: str) -> str:
        url = f"core/api/otl/schema/oef/{name}"
        content = self.requester.get(url).content
        return content.decode("utf-8")

    def get_all_eigenschappen_as_text_generator(self, size: int = 100) -> Generator[str]:
        from_ = 0

        while True:
            url = f"core/api/eigenschappen?from={from_}&size={size}"
            json_dict = self.requester.get(url).json()
            yield from json_dict['data']
            dto_list_total = json_dict['totalCount']
            from_ = json_dict['from'] + size
            if from_ >= dto_list_total:
                break
