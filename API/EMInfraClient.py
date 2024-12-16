import json
from collections.abc import Generator
from pathlib import Path

from API.EMInfraDomain import OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, PagingModeEnum, QueryDTO, BestekRef, \
    BestekKoppeling, FeedPage, AssettypeDTO, AssettypeDTOList, DTOList, AssetDTO, AssetDocumentDTO
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory
import os


class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

    def download_document(self, document: AssetDocumentDTO, directory: Path) -> Path:
        """
        Downloads a PDF document from a URL and saves it in a (temporary) folder.

        Args:
            document (AssetDocumentDTO): document object
            directory (Path): Path to the (temporary) directory.

        Returns:
            Path: The full path of the downloaded PDF file.
        """
        # Check if the directory exists, create it if it doesn't
        os.makedirs(directory, exist_ok=True)

        file_name = document.naam
        doc_link = document.document['links'][0]['href'].split('/eminfra/')[1]
        json_str = self.requester.get(doc_link).content.decode("utf-8")
        json_response = json.loads(json_str)
        doc_download_link = next(l for l in json_response['links'] if l['rel'] == 'download')['href'].split('/eminfra/')[1]
        file = self.requester.get(doc_download_link)

        with open(f'{directory}/{file_name}', 'wb') as f:
            f.write(file.content)
            print(f'Write file {file_name} to temp location: {directory}.')
            return directory / file_name

    def get_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str) -> [BestekKoppeling]:
        response = self.requester.get(
            url=f'core/api/installaties/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        print(response.json()['data'])

        return [BestekKoppeling.from_dict(item) for item in response.json()['data']]

    # def get_asset_by_bestekref(self, bestekref: str) -> [AssetDTO]:
        # response = self.requester.get(
        #     url=f'core/api/installaties/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken')
        # if response.status_code != 200:
        #     print(response)
        #     raise ProcessLookupError(response.content.decode("utf-8"))
        #
        # print(response.json()['data'])
        #
        # return [BestekKoppeling.from_dict(item) for item in response.json()['data']]

    def search_documents_by_asset_uuid(self, asset_uuid: str, query_dto: QueryDTO) -> Generator[AssetDocumentDTO]:
        """Search documents by asset uuid

        Retrieves AssetDocumentDTO associated with a specific asset_uuid, and filter the documents with a query.

        Args:
            asset_uuid: str
            query_dto: QueryDTO
            document filter
        :return:
            Generator of AssetDocumentDTO
        """
        query_dto.from_ = 0
        if query_dto.size is None:
            query_dto.size = 100
        url = f"core/api/assets/{asset_uuid}/documenten/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [AssetDocumentDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def get_documents_by_asset_uuid(self, asset_uuid: str, size: int = 10) -> Generator[AssetDocumentDTO]:
        """Get documents by asset uuid

        Retrieves all AssetDocumentDTO associated with a specific asset_uuid

        Args:
            asset_uuid: str
            size: int
            the number of document to retrieve in one API call
        :return:
            Generator of AssetDocumentDTO
        """
        _from = 0
        while True:
            url = f"core/api/assets/{asset_uuid}/documenten?from={_from}&pagingMode=OFFSET&size={size}"
            json_dict = self.requester.get(url).json()
            yield from [AssetDocumentDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            from_ = json_dict['from'] + size
            if from_ >= dto_list_total:
                break


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