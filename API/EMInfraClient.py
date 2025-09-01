import json
import logging
import uuid

from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta

from pathlib import Path
from datetime import datetime

from API.EMInfraDomain import OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, PagingModeEnum, QueryDTO, BestekRef, \
    BestekKoppeling, FeedPage, AssettypeDTO, AssettypeDTOList, DTOList, AssetDTO, BetrokkenerelatieDTO, AgentDTO, \
    PostitDTO, LogicalOpEnum, BestekCategorieEnum, BestekKoppelingStatusEnum, AssetDocumentDTO, LocatieKenmerk, \
    LogicalOpEnum, ToezichterKenmerk, IdentiteitKenmerk, AssetTypeKenmerkTypeDTO, KenmerkTypeDTO, \
    AssetTypeKenmerkTypeAddDTO, ResourceRefDTO, Eigenschap, Event, EventType, ObjectType, EventContext, ExpansionsDTO, \
    RelatieTypeDTO, KenmerkType, EigenschapValueDTO, RelatieTypeDTOList, BeheerobjectDTO, ToezichtgroepTypeEnum, \
    ToezichtgroepDTO, BaseDataclass, BeheerobjectTypeDTO, BoomstructuurAssetTypeEnum, KenmerkTypeEnum, AssetDTOToestand, \
    EigenschapValueUpdateDTO, GeometryNiveau, GeometryBron, GeometryNauwkeurigheid, GeometrieKenmerk, \
    SchadebeheerderKenmerk
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory
from utils.date_helpers import validate_dates, format_datetime
from utils.query_dto_helpers import add_expression
import os


@dataclass
class Query(BaseDataclass):
    size: int
    filters: dict
    orderByProperty: str
    fromCursor: str | None = None



class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'
        self.SCHADEBEHEERDER_UUID = 'd911dc02-f214-4f64-9c46-720dbdeb0d02'

    def _asset_kenmerk_get(self, asset_uuid: str, kenmerk_uuid: str) -> dict:
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerk_uuid}'
        resp = self.requester.get(url=url)
        if resp.status_code != 200:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())
        return resp.json()

    def _asset_kenmerk_put(self, asset_uuid: str, kenmerk_uuid: str, payload: dict) -> None:
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerk_uuid}'
        resp = self.requester.put(url=url, json=payload)
        if resp.status_code != 202:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())

    def download_document(self, document: AssetDocumentDTO, directory: Path) -> Path:
        """ Downloads document into a directory.

        Args:
            document (AssetDocumentDTO): document object
            directory (Path): Path to the (temporary) directory.

        Returns:
            Path: The full path of the downloaded PDF file.
        """
        # Check if the directory exists, create if not exist
        os.makedirs(directory, exist_ok=True)

        file_name = document.naam
        if not document.document['links']:
            raise ValueError("The 'links' list is empty.")
        doc_link = document.document['links'][0]['href'].split('/eminfra/')[1]
        json_str = self.requester.get(doc_link).content.decode("utf-8")
        json_response = json.loads(json_str)
        doc_download_link = next(l for l in json_response['links'] if l['rel'] == 'download')['href'].split('/eminfra/')[1]
        file = self.requester.get(doc_download_link)

        with open(f'{directory}/{file_name}', 'wb') as f:
            logging.info(f'Writing file {file_name} to temp location: {directory}.')
            f.write(file.content)
            return directory / file_name

    def get_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str) -> [BestekKoppeling]:
        response = self.requester.get(
            url=f'core/api/installaties/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [BestekKoppeling.from_dict(item) for item in response.json()['data']]

    def get_kenmerk_locatie_by_asset_uuid(self, asset_uuid: str) -> LocatieKenmerk:
        response = self.requester.get(
            url=f'core/api/assets/{asset_uuid}/kenmerken/80052ed4-2f91-400c-8cba-57624653db11')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return LocatieKenmerk.from_dict(response.json())

    def update_kenmerk_locatie_by_asset_uuid(self, asset_uuid: str, wkt_geom: str) -> None:
        json_body = {"geometrie": f"{wkt_geom}"}
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/80052ed4-2f91-400c-8cba-57624653db11/geometrie'
            , data=json.dumps(json_body)
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_kenmerk_geometrie(self, asset_uuid: str) -> GeometrieKenmerk:
        response = self.requester.get(url=f'core/api/assets/{asset_uuid}/kenmerken/aabe29e0-9303-45f1-839e-159d70ec2859')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return GeometrieKenmerk.from_dict(response.json())
        

    def update_geometrie_by_asset_uuid(self, asset_uuid: str, wkt_geometry: str) -> None:
        """
        Update de bestaande geometrie eigenschap door 3 hulpfuncties in te roepen:
        - get_kenmerk_geometrie()
        - delete_kenmerk_geometrie()
        - add_kenmerk_geometrie()

        :param asset_uuid:
        :param wkt_geometry:
        :return:
        """
        # step 1: search existing geometry
        geometriekenmerk = self.get_kenmerk_geometrie(asset_uuid=asset_uuid)

        # step 2: remove existing geometry
        if geometriekenmerk.logs:
            if log_id := geometriekenmerk.logs[0].uuid:
                self.delete_kenmerk_geometrie(asset_uuid, log_id)

        # step 3: add new geometry
        self.add_kenmerk_geometrie(asset_uuid=asset_uuid, wkt_geometry=wkt_geometry)

    def get_kenmerk_toezichter_by_asset_uuid(self, asset_uuid: str) -> ToezichterKenmerk:
        response = self.requester.get(
            url=f'core/api/assets/{asset_uuid}/kenmerken/f0166ba2-757c-4cf3-bf71-2e4fdff43fa3')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return ToezichterKenmerk.from_dict(response.json())

    def add_kenmerk_toezichter_by_asset_uuid(self, asset_uuid: str, toezichtgroep_uuid: str, toezichter_uuid: str) -> None:
        payload = {
            "toezichtGroep": {
                "uuid": toezichtgroep_uuid
            },
            "toezichter": {
                "uuid": toezichter_uuid
            }

        }
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/f0166ba2-757c-4cf3-bf71-2e4fdff43fa3'
            , json=payload
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_kenmerk_schadebeheerder_by_asset_uuid(self, asset_uuid: str) -> SchadebeheerderKenmerk | None:
        data = self._asset_kenmerk_get(asset_uuid, self.SCHADEBEHEERDER_UUID)
        if sb := data.get("schadeBeheerder"):
            return SchadebeheerderKenmerk.from_dict(sb)
        return None

    def get_kenmerk_schadebeheerder_by_name(self, name: str) -> SchadebeheerderKenmerk:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[TermDTO(property='naam',
                                                    operator=OperatorEnum.EQ,
                                                    value=f'{name}')])
                                 ]))
        response = self.requester.post(
            url='core/api/beheerders/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        schadebeheerders = [SchadebeheerderKenmerk.from_dict(item) for item in response.json()['data']]
        if len(schadebeheerders) != 1:
            raise ValueError(f'Expected one single schadebeheerder for {name}. Got {len(schadebeheerders)} instead.')
        return schadebeheerders[0]

    def add_kenmerk_schadebeheerder(
        self, asset_uuid: str, schadebeheerder: str = None, schadebeheerder_uuid: str = None
    ) -> None:
        """
        Toevoegen van een schadebeheerder aan een asset op basis van de naam of de uuid.
        Zodra de parameter "schadebeheerder" beschikbaar is, wordt de parameter "schadebeheerder_uuid" niet meer gebruikt.
        :param asset_uuid:
        :param schadebeheerder: naam van de schadebeerder
        :param schadebeheerder_uuid: uuid van de schadebeheerder
        :return:
        """
        if not (schadebeheerder or schadebeheerder_uuid):
            raise ValueError("één van beide (naam of UUID) is verplicht.")
        if schadebeheerder:
            schadebeheerder_uuid = self.get_kenmerk_schadebeheerder_by_name(schadebeheerder).uuid
        payload = {"schadeBeheerder": {"uuid": schadebeheerder_uuid}}
        self._asset_kenmerk_put(asset_uuid, self.SCHADEBEHEERDER_UUID, payload)

    def get_identiteit(self, toezichter_uuid: str) -> IdentiteitKenmerk:
        response = self.requester.get(
            url=f'identiteit/api/identiteiten/{toezichter_uuid}')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return IdentiteitKenmerk.from_dict(response.json())

    def get_toezichtgroep(self, toezichtGroep_uuid: str) -> ToezichtgroepDTO:
        response = self.requester.get(
            url=f'identiteit/api/toezichtgroepen/{toezichtGroep_uuid}')
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return ToezichtgroepDTO.from_dict(response.json())

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

    def get_bestekref_by_eDelta_dossiernummer(self, eDelta_dossiernummer: str) -> BestekRef | None:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaDossiernummer',
                                                    operator=OperatorEnum.EQ,
                                                    value=eDelta_dossiernummer)])]))

        response = self.requester.post('core/api/bestekrefs/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        bestekrefs_list = [BestekRef.from_dict(item) for item in response.json()['data']]
        if len(bestekrefs_list) != 1:
            raise ValueError(f'Expected one single bestek for {eDelta_dossiernummer}. Got {len(bestekrefs_list)} instead.')
        return bestekrefs_list[0]

    def get_bestekref_by_eDelta_besteknummer(self, eDelta_besteknummer: str) -> BestekRef | None:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaBesteknummer',
                                                    operator=OperatorEnum.EQ,
                                                    value=eDelta_besteknummer)])]))

        response = self.requester.post('core/api/bestekrefs/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        bestekrefs_list = [BestekRef.from_dict(item) for item in response.json()['data']]
        if len(bestekrefs_list) != 1:
            raise ValueError(f'Expected one single bestek for {eDelta_besteknummer}. Got {len(bestekrefs_list)} instead.')

        return [BestekRef.from_dict(item) for item in response.json()['data']][0]


    def change_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str, bestekkoppelingen: [BestekKoppeling]) -> None:
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken',
            data=json.dumps({'data': [item.asdict() for item in bestekkoppelingen]}))
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def adjust_date_bestekkoppeling(self, asset_uuid: str, bestek_ref_uuid: str, start_datetime: datetime = None,
                                    end_datetime: datetime = None) -> dict | None:
        """
        Adjusts the startdate and/or the enddate of an existing bestekkoppeling.

        :param asset_uuid: asset uuid
        :param bestek_ref_uuid: bestekkoppeling uuid.
        :param start_datetime: start-date of the bestekkoppeling, datetime
        :param end_datetime: end-date of the bestekkoppeling, datetime
        :return: response of the API call, or None when nothing is updated.
        """
        validate_dates(start_datetime=start_datetime, end_datetime=end_datetime)

        bestekkoppelingen = self.get_bestekkoppelingen_by_asset_uuid(asset_uuid)
        if matching_koppeling := next(
                (
                        k
                        for k in bestekkoppelingen
                        if k.bestekRef.uuid == bestek_ref_uuid
                ),
                None,
        ):
            if start_datetime:
                matching_koppeling.startDatum = format_datetime(start_datetime)
            if end_datetime:
                matching_koppeling.eindDatum = format_datetime(end_datetime)

        return self.change_bestekkoppelingen_by_asset_uuid(asset_uuid, bestekkoppelingen)

    def end_bestekkoppeling(self, asset_uuid: str, bestek_ref_uuid: str, end_datetime: datetime = datetime.now()) -> dict | None:
        """
        End a bestekkoppeling by setting an enddate. Defaults to the actual date of execution.

        :param asset_uuid: asset uuid
        :param bestek_ref_uuid: bestekkoppeling uuid.
        :param end_datetime: end-date of the bestek
        :return: response of the API call, or None when nothing is updated.
        """
        # format the end_date
        end_datetime = format_datetime(end_datetime)

        bestekkoppelingen = self.get_bestekkoppelingen_by_asset_uuid(asset_uuid)
        if matching_koppeling := next(
                (
                        k
                        for k in bestekkoppelingen
                        if k.bestekRef.uuid == bestek_ref_uuid
                ),
                None,
        ):
            matching_koppeling.eindDatum = end_datetime

        return self.change_bestekkoppelingen_by_asset_uuid(asset_uuid, bestekkoppelingen)

    def add_bestekkoppeling(self, asset_uuid: str, eDelta_besteknummer: str = None, eDelta_dossiernummer: str = None, start_datetime: datetime = datetime.now(), end_datetime: datetime = None, categorie: str = BestekCategorieEnum.WERKBESTEK, insert_index: int=0) -> dict | None:
        """
        Add a new bestekkoppeling. Start date default the execution date. End date default open-ended.
        The optional parameters "eDelta_besteknummer" and "eDelta_dossiernummer" are mutually exclusive, meaning that one of both optional parameters must be provided.

        :param asset_uuid: asset uuid
        :param eDelta_besteknummer: besteknummer
        :param eDelta_dossiernummer: dossiernummer
        :param start_datetime: start-date of the bestek. Default None > actual date.
        :param end_datetime: end-date of the bestek. Default None > open-ended.
        :param categorie: bestek categorie. Default WERKBESTEK
        :return: response of the API call, or None when nothing is updated.
        """
        if (eDelta_besteknummer is None) == (eDelta_dossiernummer is None):  # True if both are None or both are set
            raise ValueError("Exactly one of 'eDelta_besteknummer' or 'eDelta_dossiernummer' must be provided.")
        elif eDelta_besteknummer:
            new_bestekRef = self.get_bestekref_by_eDelta_besteknummer(eDelta_besteknummer=eDelta_besteknummer)
        else:
            new_bestekRef = self.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)

        # Format the start_date, or set actual date if None
        start_datetime = format_datetime(start_datetime)

        # Format the end_date if present
        if end_datetime:
            end_datetime = format_datetime(end_datetime)

        bestekkoppelingen = self.get_bestekkoppelingen_by_asset_uuid(asset_uuid)

        # Check if the new bestekkoppeling doesn't exist and append at the first index position, else do nothing
        if not (matching_koppeling := next(
                (k for k in bestekkoppelingen if k.bestekRef.uuid == new_bestekRef.uuid),
                None,)):
            new_bestekkoppeling = BestekKoppeling(
                bestekRef=new_bestekRef,
                status=BestekKoppelingStatusEnum.ACTIEF,
                startDatum=start_datetime,
                eindDatum=end_datetime,
                categorie=categorie
            )
            # Insert the new bestekkoppeling at the first index position.
            bestekkoppelingen.insert(insert_index, new_bestekkoppeling)

            return self.change_bestekkoppelingen_by_asset_uuid(asset_uuid, bestekkoppelingen)

    def replace_bestekkoppeling(self, asset_uuid: str, eDelta_besteknummer_old: str = None, eDelta_dossiernummer_old: str = None, eDelta_besteknummer_new: str = None, eDelta_dossiernummer_new: str = None, start_datetime: datetime = datetime.now(), end_datetime: datetime = None, categorie: BestekCategorieEnum = BestekCategorieEnum.WERKBESTEK) -> dict | None:
        """
        Replaces an existing bestekkoppeling: ends the existing bestekkoppeling and add a new one.

        Call the functions end_bestekkoppeling and add_bestekkoppeling respectively
        The optional parameters "eDelta_besteknummer[old|new]" and "eDelta_dossiernummer[old|new]" are mutually exclusive, meaning that one of both optional parameters must be provided.
        The optional parameter start_datetime is the enddate of the existing bestek and the start date of the new bestek. Default value is the actual date.

        :param asset_uuid: asset uuid
        :param eDelta_besteknummer_old: besteknummer existing
        :param eDelta_dossiernummer_old: dossiernummer existing
        :param eDelta_besteknummer_new: besteknummer new
        :param eDelta_dossiernummer_new: dossiernummer new
        :param start_datetime: start-date of the new bestek, and end-date of the existing bestek. Default None > actual date.
        :param end_datetime: end-date of the new bestek. Default None > open-ended.
        :param categorie: bestek categorie. Default WERKBESTEK
        :return: response of the API call, or None when nothing is updated.
        """
        # End bestekkoppeling
        if (eDelta_besteknummer_old is None) == (eDelta_dossiernummer_old is None):  # True if both are None or both are set
            raise ValueError("Exactly one of 'eDelta_besteknummer_old' or 'eDelta_dossiernummer_old' must be provided.")
        elif eDelta_besteknummer_old:
            bestekref = self.get_bestekref_by_eDelta_besteknummer(eDelta_besteknummer=eDelta_besteknummer_old)
        else:
            bestekref = self.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer_old)

        self.end_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref.uuid, end_datetime=start_datetime)

        # Add bestekkoppeling
        if (eDelta_besteknummer_new is None) == (eDelta_dossiernummer_new is None):  # True if both are None or both are set
            raise ValueError("Exactly one of 'eDelta_besteknummer_new' or 'eDelta_dossiernummer_new' must be provided.")
        else:
            self.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_besteknummer=eDelta_besteknummer_new,
                                     eDelta_dossiernummer=eDelta_dossiernummer_new, start_datetime=start_datetime,
                                     end_datetime=end_datetime, categorie=categorie)

    def get_feedproxy_page(self, feed_name: str, page_num: int, page_size: int = 1):
        url = f"feedproxy/feed/{feed_name}/{page_num}/{page_size}"
        json_dict = self.requester.get(url).json()
        return FeedPage.from_dict(json_dict)

    def get_assettype_by_id(self, assettype_id: str) -> AssettypeDTO:
        url = f"core/api/assettypes/{assettype_id}"
        json_dict = self.requester.get(url).json()
        return AssettypeDTO.from_dict(json_dict)

    def search_assettype(self, uri: str) -> AssettypeDTO:
        """
        Searches assettype based on the URI. One single Assettype is returned, based on an exact search of the URI.
        :param uri:
        :return:
        """
        query_dto = QueryDTO(
            size=1,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[ExpressionDTO(
                    terms=[
                        TermDTO(property='uri', operator=OperatorEnum.EQ, value=uri)
                    ])
                ])
        )
        url = "core/api/assettypes/search"
        json_dict = self.requester.post(url, data=query_dto.json()).json()
        assettypes = [AssettypeDTO.from_dict(item) for item in json_dict['data']]
        if len(assettypes) != 1:
            raise ValueError(f'Exactly one Assettype should be returned when searching for uri: "{uri}" Check URI.')
        return assettypes[0]


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

    def get_beheerobject_by_uuid(self, beheerobject_uuid: str) -> BeheerobjectDTO:
        url = f"core/api/beheerobjecten/{beheerobject_uuid}"
        json_dict = self.requester.get(url).json()
        return BeheerobjectDTO.from_dict(json_dict)

    def get_assets_by_filter(self, filter: dict, size: int = 100, order_by_property: str = None) -> Generator[dict]:
        """filter for otl/assets/search"""
        yield from self.get_objects_from_oslo_search_endpoint(url_part='assets', filter_dict=filter, size=size)

    def _search_assets_helper(self, query_dto: QueryDTO) -> Generator[AssetDTO]:
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

    def search_assets(self, query_dto: QueryDTO, actief:bool = None) -> Generator[AssetDTO]:
        if actief is not None:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    terms=[TermDTO(property='actief',
                                   operator=OperatorEnum.EQ,
                                   value=actief)
                           ]
                    , logicalOp=LogicalOpEnum.AND)
            )
        yield from self._search_assets_helper(query_dto)

    def search_parent_asset(
            self,
            asset_uuid: str,
            recursive: bool = False,
            return_all_parents: bool = False
    ) -> AssetDTO | list[AssetDTO] | None:
        """
        Search for the parent asset(s) of a given asset UUID.

        :param asset_uuid: UUID of the asset to search the parent for.
        :param recursive: If True, search recursively up the parent chain.
        :param return_all_parents: If True, return a list of all parents; if False, return only the final parent.
        :return: A single AssetDTO, a list of AssetDTOs, or None if no parent found.
        """
        query_dto = QueryDTO(
            size=1,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            expansions=ExpansionsDTO(fields=['parent']),
            selection=SelectionDTO(
                expressions=[ExpressionDTO(
                    terms=[
                        TermDTO(property='id', operator=OperatorEnum.EQ, value=asset_uuid)
                    ])
                ])
        )
        url = "core/api/assets/search"
        json_dict = self.requester.post(url, data=query_dto.json()).json()

        if not json_dict['data']:
            return None  # No data found

        asset_data = json_dict['data'][0]
        parent_data = asset_data.get('parent')

        if parent_data is None:
            return None  # No parent found

        parent_asset = AssetDTO.from_dict(parent_data)

        if not recursive:
            # Only return the immediate parent
            return parent_asset

        # Recursive case
        parents = [parent_asset]
        next_parent = self.search_parent_asset(parent_asset.uuid, recursive=True, return_all_parents=True)

        if next_parent:
            if isinstance(next_parent, list):
                parents.extend(next_parent)
            else:
                parents.append(next_parent)

        if return_all_parents:
            return parents
        else:
            return parents[-1]  # Return only the last parent (the top-most one)

    def search_child_assets(self, asset_uuid: str, recursive: bool = False) -> Generator[AssetDTO] | None:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent']),
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[
                                         TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)
                                     ])]))
        url = f"core/api/assets/{asset_uuid}/assets/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            assets = [AssetDTO.from_dict(item) for item in json_dict['data']]
            for asset in assets:
                yield asset # yield the current asset
                # If recursive, call recursively for each asset's uuid
                if recursive:
                    yield from self.search_child_assets(asset_uuid=asset.uuid, recursive=True)
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_relaties(self, assetId: str, kenmerkTypeId: str, relatieTypeId: str) -> Generator[RelatieTypeDTO]:
        url = f"core/api/assets/{assetId}/kenmerken/{kenmerkTypeId}/assets-via/{relatieTypeId}"
        json_dict = self.requester.get(url).json()
        yield from [RelatieTypeDTO.from_dict(item) for item in json_dict['data']]

    def search_asset_by_uuid(self, asset_uuid: str) -> Generator[AssetDTO]:
        """
        Search active and inactive assets by its name.

        :param asset_uuid:
        :return:
        """
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent']),
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[
                                         TermDTO(property='id', operator=OperatorEnum.EQ, value=asset_uuid)
                                     ])]))
        yield from self._search_assets_helper(query_dto)

    def search_asset_by_name(self, asset_name: str, exact_search: bool = True) -> Generator[AssetDTO]:
        """
        Search active and inactive assets by its name.
        Exact_search (default True) searches for an exact match operator EQUALS, while exact_search = False loosely searches using operator CONTAINS.

        :param name:
        :param exact_search:
        :return:
        """
        operator = OperatorEnum.EQ if exact_search else OperatorEnum.CONTAINS
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent']),
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[
                                         TermDTO(property='naam', operator=operator, value=asset_name)
                                     ])]))
        yield from self._search_assets_helper(query_dto)

    def search_all_assets(self, query_dto: QueryDTO) -> Generator[AssetDTO]:
        yield from self._search_assets_helper(query_dto)

    def create_asset(self, parent_uuid: str, naam: str, typeUuid: str, parent_asset_type:BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict | None:
        """
        Create an asset in the arborescence
        :param parent_uuid: asset uuid van de parent-asset
        :param naam: naam van de nieuw aan te maken child-asset
        :param typeUuid: assettype van het nieuw aan te maken child-asset
        :return:
        """
        json_body = {
            "naam": naam,
            "typeUuid": typeUuid
        }

        if parent_asset_type.value == 'asset':
            prefix = 'assets'
        elif parent_asset_type.value == 'beheerobject':
            prefix = 'beheerobjecten'
        else:
            raise ValueError(f"Unexpected parent_asset_type: {parent_asset_type.value}")

        url = f'core/api/{prefix}/{parent_uuid}/assets'

        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()


    def get_all_eventtypes(self) -> Generator[EventType]:
        url = f"core/api/events/eventtypes"
        json_dict = self.requester.get(url).json()
        yield from [EventType.from_dict(item) for item in json_dict['data']]

    def search_events(self, asset_uuid: str = None, created_after: datetime = None, created_before: datetime = None, created_by: IdentiteitKenmerk = None, event_type: EventType = None, event_context: EventContext = None) -> Generator[Event]:
        """
        Search the history of em-infra, called events

        :param asset_uuid: asset identificator
        :param created_after: date after which the asset was edited
        :param created_before: date before the asset was edited
        :param created_by: person who created the asset
        :param event_type: type of event
        :param event_context: context of the event
        :return: A generator yielding Event objects.
        """
        if all(p is None for p in (asset_uuid, created_after, created_before, created_by, event_type, event_context)):
            raise ValueError("At least one parameter must be provided.")

        query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET, selection=SelectionDTO(expressions=[]))

        if asset_uuid:
            expression = ExpressionDTO(terms=[TermDTO(property='objectId', operator=OperatorEnum.EQ, value=f'{asset_uuid}')], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if created_after:
            expression = ExpressionDTO(terms=[TermDTO(property='createdOn', operator=OperatorEnum.GTE, value=format_datetime(created_after))], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if created_before:
            # workaround: add 1 day and set the operator to strictly lower than.
            created_before += timedelta(days=1)
            expression = ExpressionDTO(terms=[TermDTO(property='createdOn', operator=OperatorEnum.LT, value=format_datetime(created_before))], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if created_by:
            expression = ExpressionDTO(terms=[TermDTO(property='createdBy', operator=OperatorEnum.EQ, value=created_by.uuid)], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if event_type:
            expression = ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=event_type.name)], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if event_context:
            expression = ExpressionDTO(terms=[TermDTO(property='contextId', operator=OperatorEnum.EQ, value=event_context.uuid)], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        # Set logical operator to None for the first term of the expression
        query_dto.selection.expressions[0].logicalOp = None

        url = "core/api/events/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [Event.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_identiteit(self, naam: str) -> Generator[IdentiteitKenmerk]:
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True, logicalOp=None)]
                                         , logicalOp=None
                                     )
                                 ]
                             )
                             )

        naam_parts = naam.split(' ')
        for naam_part in naam_parts:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    terms=[
                        TermDTO(property='naam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}', logicalOp=None)
                        , TermDTO(property='voornaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}', logicalOp=LogicalOpEnum.OR)
                        , TermDTO(property='roepnaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}', logicalOp=LogicalOpEnum.OR)
                        , TermDTO(property='gebruikersnaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}', logicalOp=LogicalOpEnum.OR)
                    ]
                    , logicalOp=LogicalOpEnum.AND
                )
            )

        query_dto.from_ = 0
        if query_dto.size is None:
            query_dto.size = 100

        url = "identiteit/api/identiteiten/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [IdentiteitKenmerk.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_eventcontexts(self, omschrijving: str = None) -> Generator[EventContext]:
        if omschrijving:
            query_dto = QueryDTO(size=100,
                                 from_=0,
                                 orderByProperty='omschrijving',
                                 pagingMode=PagingModeEnum.OFFSET,
                                 selection=SelectionDTO(
                                     expressions=[
                                         ExpressionDTO(
                                             terms=[TermDTO(property='omschrijving', operator=OperatorEnum.CONTAINS, value=f'{omschrijving}',
                                                            logicalOp=None)]
                                             , logicalOp=None)]))

        url = "core/api/eventcontexts/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [EventContext.from_dict(item) for item in json_dict['data']]
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

    def search_postits(self, asset_uuid: str, startDatum: datetime = None, eindDatum: datetime = None) -> Generator[PostitDTO] | None:
        """
        Search postits of an asset.
        If the optional parameters startDatum or eindDatum are missing, return all postits.

        :param asset_uuid: asset_uuid
        :param startDatum: start date of the postit, default None
        :param eindDatum: eind date of the postit, default None
        :return: Generator[PostitDTO] or None
        """
        # intiate empty expression
        query_dto = QueryDTO(
            size=5,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[]
            )
        )

        if startDatum:
            add_expression(query_dto, 'startDatum', OperatorEnum.GTE, startDatum)

        if eindDatum:
            add_expression(query_dto, 'eindDatum', OperatorEnum.LTE, eindDatum)

        # If both dates are present, add logical AND
        if startDatum and eindDatum:
            query_dto.selection.expressions[-1].logicalOp = LogicalOpEnum.AND

        if query_dto.size is None:
            query_dto.size = 100
        url = f"core/api/assets/{asset_uuid}/postits/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [PostitDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def get_postit(self, asset_uuid: str, postit_uuid: str) -> Generator[PostitDTO] | None:
        """
        Search one postit of an asset.

        :param asset_uuid: asset_uuid
        :param postit_uuid: postit_uuid
        :return: Generator[PostitDTO] or None
        """
        url = f"core/api/assets/{asset_uuid}/postits/{postit_uuid}"

        response = self.requester.get(url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return PostitDTO.from_dict(response.json())

    def add_postit(self, asset_uuid: str,  commentaar: str, startDatum: datetime, eindDatum: datetime) -> dict:
        """
        Add postit to an asset.

        :param asset_uuid: asset_uuid
        :param commentaar: comment
        :param startDatum: start date of the postit
        :param eindDatum: end date of the postit
        :return: dict
        """
        validate_dates(start_datetime=startDatum, end_datetime=eindDatum)

        startDatum_str = format_datetime(startDatum)
        eindDatum_str = format_datetime(eindDatum)

        json_body = {
            "commentaar": commentaar,
            "startDatum": startDatum_str,
            "eindDatum": eindDatum_str
        }

        url = f"core/api/assets/{asset_uuid}/postits"
        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def edit_postit(self, asset_uuid: str,  postit_uuid: str, commentaar: str = None, startDatum: datetime = None, eindDatum: datetime = None) -> dict:
        """
        Edit postit of an asset.
        Although mandatory in the API Call, the parameters commentaar, startDatum and eindDatum are optional.
        When missing, the actual values are used

        Also used to perform a safe-delete, by altering only the parameter eindDatum.

        :param asset_uuid: asset_uuid
        :param postit_uuid: postit_uuid
        :param commentaar: comment
        :param startDatum: start date of the postit
        :param eindDatum: end date of the postit
        :return: dict
        """
        if startDatum and eindDatum:
            validate_dates(start_datetime=startDatum, end_datetime=eindDatum)

        actual_postit = self.get_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid)
        actual_commentaar = actual_postit.commentaar
        actual_startDatum = actual_postit.startDatum
        actual_eindDatum = actual_postit.eindDatum

        json_body = {"commentaar": commentaar if commentaar else actual_commentaar}

        if startDatum:
            startDatum_str = format_datetime(startDatum)
            json_body["startDatum"] = startDatum_str
        else:
            json_body["startDatum"] = actual_startDatum

        if eindDatum:
            eindDatum_str = format_datetime(eindDatum)
            json_body["eindDatum"] = eindDatum_str
        else:
            json_body["eindDatum"] = actual_eindDatum

        url = f"core/api/assets/{asset_uuid}/postits/{postit_uuid}"
        response = self.requester.put(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def remove_postit(self, asset_uuid: str, postit_uuid: str):
        """
        Remove postit of an asset.

        :param asset_uuid: asset_uuid
        :param postit_uuid: postit_uuid
        :return: dict
        """
        json_body = {
            "uuids": [f"{postit_uuid}"]
        }

        url = f"core/api/assets/{asset_uuid}/postits/ops/remove"
        response = self.requester.put(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response

    def get_objects_from_oslo_search_endpoint(self, url_part: str,
                                              filter_dict: dict = '{}', size: int = 100,
                                              expansions_fields: [str] = None) -> Generator:
        """Returns Generator objects for each OSLO endpoint

        :param url_part: keyword to complete the url
        :type url_part: str
        :param filter_dict: filter condition
        :type filter_dict: dict
        :param size: amount of objects to return in 1 page or request
        :type size: int
        :param expansions_fields: additional fields to append to the results
        :type expansions_fields: [str]
        :return: Generator
        """
        body = {'size': size, 'fromCursor': None, 'filters': filter_dict}
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

    def remove_parent_from_asset(self, parent_uuid: str, asset_uuid: str):
        """Removes the parent from an asset.

        :param parent_uuid: The UUID of the parent asset.
        :type parent_uuid: str
        :param asset_uuid: The UUID of the asset to remove the parent from.
        :type asset_uuid: str
        """
        payload = {
            "name": "remove",
            "description": "Verwijderen uit boomstructuur van 1 asset",
            "async": False,
            "uuids": [asset_uuid],
        }
        url=f"core/api/beheerobjecten/{parent_uuid}/assets/ops/remove"
        response = self.requester.put(
            url=url,
            json=payload
        )
        if response.status_code != 202:
            ProcessLookupError(f'Failed to remove parent from asset: {response.text}')

    def search_agent(self, naam: str, ovoCode: str = None, actief: bool = True) -> Generator[AgentDTO]:
        query_dto = QueryDTO(
            size=100,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            expansions={"fields": ["contactInfo"]},
            selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=naam)])
                ]
            )
        )
        if ovoCode:
            query_dto.selection.expressions.append(
                ExpressionDTO(terms=[TermDTO(property='ovoCode', operator=OperatorEnum.EQ, value=ovoCode)]
                              , logicalOp=LogicalOpEnum.AND)
            )
        if actief is not None:
            query_dto.selection.expressions.append(
                ExpressionDTO(terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=actief)]
                              , logicalOp=LogicalOpEnum.AND)
            )
        url = "core/api/agents/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [AgentDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def add_betrokkenerelatie(self, asset: AssetDTO, agent_uuid: str, rol: str) -> dict:
        json_body = {
            "bron": {
                "uuid": f"{asset.uuid}"
                , "_type": f"{asset._type}"
            },
            "doel": {
                "uuid": f"{agent_uuid}"
                , "_type": "agent"
            },
            "rol": f"{rol}"
        }
        response = self.requester.post(url='core/api/betrokkenerelaties', json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def remove_betrokkenerelatie(self, betrokkenerelatie_uuid: str) -> dict:
        url = f"core/api/betrokkenerelaties/{betrokkenerelatie_uuid}"
        response = self.requester.delete(url=url)
        if response.status_code != 202:
            logging.error(response)
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

    def search_eigenschappen(self, eigenschap_naam: str, uri: str = None) -> [Eigenschap]:
        """
        Search Eigenschap with "eigenschap_naam" (mandatory) and "uri" (optional) parameters.
        The optional parameter "uri" results in one single list item Eigenschap.
        This is usefull when searching for a common eigenschap like e.g. 'merk', 'hoogte',

        :param eigenschap_naam:
        :param uri:
        :return:
        """
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='naam',
                                                    operator=OperatorEnum.EQ,
                                                    value=eigenschap_naam)])]))

        if uri:
            expression_uri = ExpressionDTO(
                                     terms=[TermDTO(property='uri',
                                                    operator=OperatorEnum.CONTAINS,
                                                    value=uri)], logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression_uri)

        response = self.requester.post('core/api/eigenschappen/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [Eigenschap.from_dict(item) for item in response.json()['data']]

    def search_eigenschapwaarden(self, assetId: str) -> [EigenschapValueDTO]:
        # 'https://services.apps-tei.mow.vlaanderen.be/eminfra/core/api/assets/efc780ec-ad42-4df6-8cf6-3233756f5832/kenmerken/cef6a3c0-fd1b-48c3-8ee0-f723e55dd02b/eigenschapwaarden'
        url = f'core/api/assets/{assetId}/kenmerken/cef6a3c0-fd1b-48c3-8ee0-f723e55dd02b/eigenschapwaarden'
        response = self.requester.post(url=url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [EigenschapValueDTO.from_dict(item) for item in response.json()['data']]

    def search_toezichtgroep_lgc(self, naam: str, type: ToezichtgroepTypeEnum = None) -> Generator[
        ToezichtgroepDTO]:  # todo wijzig dict naar een toezichtgroep object
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[
                                             TermDTO(property='naam',
                                                     operator=OperatorEnum.EQ,
                                                     value=naam),
                                             TermDTO(property='referentie',
                                                     operator=OperatorEnum.EQ,
                                                     value=naam,
                                                     logicalOp=LogicalOpEnum.OR),
                                         ])
                                 ]))
        if type:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    terms=[
                        TermDTO(property='type', operator=OperatorEnum.EQ, value=type)]
                    , logicalOp=LogicalOpEnum.AND))
        url = "identiteit/api/toezichtgroepen/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [ToezichtgroepDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def get_kenmerken_by_assettype_uuid(self, uuid: str) -> [AssetTypeKenmerkTypeDTO]:
        url = f"core/api/assettypes/{uuid}/kenmerktypes"
        json_dict = self.requester.get(url).json()
        return [AssetTypeKenmerkTypeDTO.from_dict(item) for item in json_dict['data']]

    def get_kenmerktype_by_naam(self, naam: str) -> KenmerkTypeDTO:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='naam',
                                                    operator=OperatorEnum.EQ,
                                                    value=naam)])]))

        response = self.requester.post('core/api/kenmerktypes/search', data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return next(KenmerkTypeDTO.from_dict(item) for item in response.json()['data'])

    def add_kenmerk_to_assettype(self, assettype_uuid: str, kenmerktype_uuid: str) -> None:
        r = ResourceRefDTO(uuid=kenmerktype_uuid)
        add_dto = AssetTypeKenmerkTypeAddDTO(kenmerkType=ResourceRefDTO(uuid=kenmerktype_uuid))
        response = self.requester.post(f'core/api/assettypes/{assettype_uuid}/kenmerktypes', data=add_dto.json())
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_eigenschap(self, assetId: str, eigenschap: EigenschapValueDTO | EigenschapValueUpdateDTO) -> None:
        """
        Updates an eigenschap value on an asset, handling both DTO types.

        :param assetId: The ID of the asset.
        :param eigenschap: Either an EigenschapValueDTO or EigenschapValueUpdateDTO.
        """
        request_body = {
            "data": [
                {
                    "eigenschap": eigenschap.eigenschap.asdict(),
                    "typedValue": eigenschap.typedValue
                }]
        }

        # Determine how to retrieve the UUID for the kenmerk
        if hasattr(eigenschap, "kenmerkType") and hasattr(eigenschap.kenmerkType, "uuid"):
            kenmerk_uuid = eigenschap.kenmerkType.uuid
        else:
            kenmerk_eigenschap = self.get_kenmerken(assetId=assetId, naam=KenmerkTypeEnum.EIGENSCHAPPEN)
            kenmerk_uuid = kenmerk_eigenschap.type.get("uuid", None)

        response = self.requester.patch(url=f'core/api/assets/{assetId}/kenmerken/{kenmerk_uuid}/eigenschapwaarden', json=request_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def list_eigenschap(self, kenmerktypeId: str) -> [Eigenschap]:
        url = f"core/api/kenmerkypes/{kenmerktypeId}/eigenschappen"
        json_dict = self.requester.get(url).json()
        return [Eigenschap.from_dict(item) for item in json_dict['data']]

    def update_kenmerk(self, asset_uuid: str, kenmerk_uuid: str, request_body: dict) -> None:
        response = self.requester.put(url=f'core/api/assets/{asset_uuid}/kenmerken/{kenmerk_uuid}', json=request_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_toestand(self, asset: AssetDTO, toestand: AssetDTOToestand = AssetDTOToestand.IN_ONTWERP, actief:bool=True) -> None:
        """
        Update toestand of an asset. Keep the asset's naam, commentaar.
        Set default actief = True

        :param asset:
        :param toestand:
        :return:
        """
        request_body = {
            "naam": asset.naam,
            "actief": actief,
            "toestand": toestand.value,
            "commentaar": asset.commentaar
        }
        response = self.requester.put(url=f'core/api/assets/{asset.uuid}', json=request_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_kenmerktype_and_relatietype_id(self, relatie_uri: str) -> (str, str):
        relaties_dict = {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing": [
                "3e207d7c-26cd-468b-843c-6648c7eeebe4",
                "93c88f93-6e8c-4af3-a723-7e7a6d6956ac"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsNetwerkECC": [
                "",
                "41c7e2eb-17be-4f53-a49e-0f3bc31efdd0"
            ],
            "https://grp.data.wegenenverkeer.be/ns/onderdeel#DeelVan": [
                "",
                "afbe8124-a9e2-41b9-a944-c14a41a9f4d5"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#SluitAanOp": [
                "",
                "b4e89ae7-cb69-449c-946b-fdff13f63a7a"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt": [
                "91d6223c-c5d7-4917-9093-f9dc8c68dd3e",
                "f2c5c4a1-0899-4053-b3b3-2d662c717b44"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsSWOnderdeelVan": [
                "",
                "1aa9795c-7ed0-4d96-87b9-e51159055755"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsAdmOnderdeelVan": [
                "",
                "dcc18707-2ca1-4b35-bfff-9fa262da96dd"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij": [
                "8355857b-8892-45a5-a86b-6375b797c764",
                "812dd4f3-c34e-43d1-88f1-3bcd0b1e89c2"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VoedtAangestuurd": [
                "",
                "a6747802-7679-473f-b2bd-db2cfd1b88d7"
            ],
            "https://bz.data.wegenenverkeer.be/ns/onderdeel#Bezoekt": [
                "",
                "e801b062-74e1-4b39-9401-163dd91d5494"
            ],
            "https://bz.data.wegenenverkeer.be/ns/onderdeel#HeeftBeheeractie": [
                "",
                "cd5104b3-5e98-4055-8af2-5724bf141e44"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijlage": [
                "",
                "e7d8e795-06ef-4e0f-b049-c736b54447c9"
            ],
            "https://bz.data.wegenenverkeer.be/ns/onderdeel#IsAanleiding": [
                "",
                "fef0df58-8243-4869-a056-a71346bf6acd"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsSWGehostOp": [
                "",
                "20b29934-fd5e-490f-a94b-e566513be407"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Omhult": [
                "",
                "e2c644ec-7fbd-48ff-906a-4747b43b11a5"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#LigtOp": [
                "",
                "321c18b8-92ca-4188-a28a-f00cdfaa0e31"
            ],
            "https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar": [
                "",
                "f0ed1efa-fe29-4861-89dc-5d3bc40f0894"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBeheer": [
                "",
                "6c91fe94-8e29-4906-a02c-b8507495ad21"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging": [
                "c3494ff0-9e02-4c11-856c-da8db6238768",
                "3ff9bf1c-d852-442e-a044-6200fe064b20"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftNetwerktoegang": [
                "",
                "3a63adb8-493a-4aa8-8e2e-164fd942b0b9"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftToegangsprocedure": [
                "",
                "0da67bde-0152-445f-8f29-6a9319f890fd"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftNetwerkProtectie": [
                "",
                "34d043f5-583d-4c1e-9f99-4d89fcb84ef4"
            ],
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftAanvullendeGeometrie": [
                "",
                "de86510a-d61c-46fb-805d-c04c78b27ab6"
            ]
        }
        return relaties_dict[relatie_uri]

    def add_relatie(self, assetId: str, kenmerkTypeId: str, relatieTypeId: str, doel_assetId: str) -> None:
        """
            https://apps.mow.vlaanderen.be/eminfra/core/swagger-ui/#/kenmerk/addRelatie_40

        :param assetId:
        :param kenmerkTypeId: 91d6223c-c5d7-4917-9093-f9dc8c68dd3e
        :param relatieTypeId: f2c5c4a1-0899-4053-b3b3-2d662c717b44
        :return:
        """
        _target_asset = self.get_asset_by_id(assettype_id=doel_assetId)
        _type = _target_asset._type
        if _type == 'installatie':
            relatie_type = 'installaties-via'
        else:
            raise ValueError(f'Type of the "doel_assetId" {doel_assetId} can not be determined')

        json_body = {"uuid": doel_assetId}
        url = f'core/api/assets/{assetId}/kenmerken/{kenmerkTypeId}/{relatie_type}/{relatieTypeId}'
        response = self.requester.post(url=url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def create_assetrelatie(self, bronAsset_uuid: str, doelAsset_uuid: str, relatieType_uuid: str) -> str:
        bron_asset = next(self.search_asset_by_uuid(asset_uuid=bronAsset_uuid), None)
        doel_asset = next(self.search_asset_by_uuid(asset_uuid=doelAsset_uuid), None)

        json_body = {
            "bronAsset": {
                "uuid": f"{bronAsset_uuid}",
                "_type": f"{bron_asset._type}"
            },
            "doelAsset": {
                "uuid": f"{doelAsset_uuid}",
                "_type": f"{doel_asset._type}"
            },
            "relatieType": {
                "uuid": f"{relatieType_uuid}"
            }
        }
        url = 'core/api/assetrelaties'
        response = self.requester.post(url=url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json().get("uuid")

    def search_assetrelaties_OTL(self, bronAsset_uuid: str = None, doelAsset_uuid: str = None) -> dict:
        if bronAsset_uuid is None and doelAsset_uuid is None:
            raise ValueError('At least one optional parameter "bronAsset_uuid" or "doelAsset_uuid" must be provided.')
        if bronAsset_uuid:
            try:
                uuid.UUID(bronAsset_uuid)
            except ValueError:
                raise ValueError('Invalid format for bronAsset_uuid; must be a valid UUID string.')
        if doelAsset_uuid:
            try:
                uuid.UUID(doelAsset_uuid)
            except ValueError:
                raise ValueError('Invalid format for doelAsset_uuid; must be a valid UUID string.')
        json_body = {"filters": {}}
        if bronAsset_uuid:
            json_body["filters"]["bronAsset"] = bronAsset_uuid
        if doelAsset_uuid:
            json_body["filters"]["doelAsset"] = doelAsset_uuid
        url = 'core/api/otl/assetrelaties/search'
        response = self.requester.post(url=url, json=json_body)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json().get("@graph")

    def get_kenmerken(self, assetId: str, naam: KenmerkTypeEnum = None) -> list[KenmerkType] | KenmerkType:
        """

        :param assetId:
        :param naam: Naam van het kenmerk. Default None, returns all Kenmerken.
        :return:
        """
        url = f'core/api/assets/{assetId}/kenmerken'
        response = self.requester.get(url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        all_kenmerken = [KenmerkType.from_dict(item) for item in response.json()['data']]
        if naam:
            all_kenmerken = [item for item in all_kenmerken if item.type.get("naam").startswith(naam.value)][0]
        return all_kenmerken

    def get_eigenschappen(self, assetId: str) -> list[EigenschapValueDTO]:
        # ophalen kenmerk_uuid
        kenmerken = self.get_kenmerken(assetId=assetId)
        kenmerk_uuid = [kenmerk.type.get('uuid') for kenmerk in kenmerken if kenmerk.type.get('naam').startswith('Eigenschappen')][0]

        # ophalen eigenschapwaarden
        url = f'core/api/assets/{assetId}/kenmerken/{kenmerk_uuid}/eigenschapwaarden'
        json_dict = self.requester.get(url).json()
        return [EigenschapValueDTO.from_dict(item) for item in json_dict['data']]

    def get_eigenschapwaarden(self, assetId: str, eigenschap_naam: str = None) -> list[EigenschapValueDTO]:
        url = f'core/api/assets/{assetId}/kenmerken/753c1268-68c2-4e67-a6cc-62c0622b576b/eigenschapwaarden'
        json_dict = self.requester.get(url).json()
        eigenschap_value_list = [EigenschapValueDTO.from_dict(item) for item in json_dict['data']]
        if eigenschap_naam:
            eigenschap_value_list = [item for item in eigenschap_value_list if item.eigenschap.naam == eigenschap_naam]
        return eigenschap_value_list

    def search_beheerobjecten(self, naam: str, beheerobjecttype: BeheerobjectTypeDTO = None, actief: bool = None, operator: OperatorEnum = OperatorEnum.CONTAINS) -> Generator[BeheerobjectDTO]:
        query_dto = QueryDTO(
            size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[ExpressionDTO(
                    terms=[TermDTO(property='naam', operator=operator, value=naam)])]))

        if beheerobjecttype:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    logicalOp=LogicalOpEnum.AND
                    , terms=[TermDTO(property='type',operator=OperatorEnum.EQ,value=beheerobjecttype.uuid)])
            )

        if actief or not actief:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    logicalOp=LogicalOpEnum.AND
                    , terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=actief)])
            )

        url = 'core/api/beheerobjecten/search'
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [BeheerobjectDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def get_beheerobjecttypes(self) -> list[BeheerobjectTypeDTO]:
        url = 'core/api/beheerobjecttypes'
        json_dict = self.requester.get(url).json()
        return [BeheerobjectTypeDTO.from_dict(item) for item in json_dict['data']]

    def create_beheerobject(self, naam: str, beheerobjecttype: BeheerobjectTypeDTO = None) -> dict | None:
        """

        :param naam:
        :param beheerobjecttype: Optional parameter. Set default value to installatie when missing
        :return:
        """
        if beheerobjecttype is None:
            beheerobjecttypes = self.get_beheerobjecttypes()
            default_beheerobjecttype = [item for item in beheerobjecttypes if item.naam == 'INSTAL (Beheerobject)']
            if not default_beheerobjecttype:
                raise ValueError("Default beheerobjecttype 'INSTAL (Beheerobject)' not found")
            beheerobjecttype = default_beheerobjecttype[0]
        json_body = {
            "naam": naam,
            "typeUuid": beheerobjecttype.uuid
        }
        url = 'core/api/beheerobjecten'
        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def activeer_asset(self, asset: AssetDTO) -> dict:
        json_body = {
            "actief": True
            , "naam": asset.naam
        }
        response = self.requester.put(
            url=f'core/api/assets/{asset.uuid}'
            , json=json_body
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def deactiveer_asset(self, asset: AssetDTO) -> dict:
        json_body = {
            "actief": False
            , "naam": asset.naam
        }
        response = self.requester.put(
            url=f'core/api/assets/{asset.uuid}'
            , json=json_body
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def reorganize_beheerobject(self, parentAsset: AssetDTO, childAsset: AssetDTO, parentType:BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict:
        """
        Assets verplaatsen in de boomstructuur met 1 parent en 1 child-asset.

        :param parentAsset:
        :param childAsset:
        :return:
        """
        json_body = {
            "name":"reorganize",
            "moveOperations":
                [{"assetsUuids":[f'{childAsset.uuid}']
                     ,"targetType":parentType.value
                     ,"targetUuid":f'{parentAsset.uuid}'}]
        }
        response = self.requester.put(
            url='core/api/beheerobjecten/ops/reorganize'
            , json=json_body
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_asset(self, uuid:str, naam:str, toestand:str, commentaar:str = None, isActief:bool = None):
        json_body = {
            "actief": isActief, "naam": naam, "toestand": toestand, "commentaar": commentaar
        }
        response = self.requester.put(
            url=f'core/api/assets/{uuid}'
            , json=json_body
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def zoek_verweven_asset(self, bron_uuid: str) -> AssetDTO | None:
        """
        Zoek de OTL-asset op basis van een Legacy-asset die verbonden zijn via een Gemigreerd-relatie.
        Returns None indien de Gemigreerd-relatie ontbreekt.

        :param bron_uuid: uuid van de bron asset (Legacy)
        :return:
        """
        relaties = self.search_assetrelaties_OTL(bronAsset_uuid=bron_uuid)
        relatie_gemigreerd = next((r for r in relaties if r.get('@type') == 'https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar'), None)
        asset_uuid_gemigreerd = relatie_gemigreerd.get('RelatieObject.doelAssetId').get(
            'DtcIdentificator.identificator')[:36]
        return next(
            self.search_asset_by_uuid(asset_uuid=asset_uuid_gemigreerd),
            None,
        )

    def create_onderdeel(self, naam: str, typeUuid: str) -> dict | None:
        json_body = {
            "naam": naam,
            "typeUuid": typeUuid
        }
        url = 'core/api/onderdelen'
        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def delete_kenmerk_geometrie(self, asset_uuid: str, log_id: str):
        response = self.requester.delete(
            url=f'core/api/assets/{asset_uuid}/kenmerken/aabe29e0-9303-45f1-839e-159d70ec2859/logs/{log_id}')
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def add_kenmerk_geometrie(self, asset_uuid, wkt_geometry, geometry_log:GeometryNiveau = GeometryNiveau.MIN_1
            , geometry_bron:GeometryBron = GeometryBron.MANUEEL
            , geometry_nauwkeurigheid:GeometryNauwkeurigheid = GeometryNauwkeurigheid._50):
        """

        :param asset_uuid:
        :param wkt_geometry: Well Known Text geometrie
        :param geometry_log: default waarde "-1"
        :param geometry_bron: default waarde "Manueel"
        :param geometry_nauwkeurigheid: default waarde "<50 cm"
        :return:
        """
        json_body = {
            "wkt": f"{wkt_geometry}",
            "niveau": f"{geometry_log.value}",
            "nauwkeurigheid": f"{geometry_nauwkeurigheid.value}",
            "bron": f"{geometry_bron.value}"
        }
        response = self.requester.post(
            url=f'core/api/assets/{asset_uuid}/kenmerken/aabe29e0-9303-45f1-839e-159d70ec2859/logs'
            , data=json.dumps(json_body)
        )
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
