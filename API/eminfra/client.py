import json
import logging
import uuid

from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta

from pathlib import Path

from API.eminfra.assets import AssetService
from API.eminfra.assettypes import AssettypesService
from API.eminfra.beheerobject import BeheerobjectService
from API.eminfra.bestekken import BestekService
from API.eminfra.documenten import DocumentService
from API.eminfra.events import EventService
from API.eminfra.feed import FeedService
from API.eminfra.geometrie import GeometrieService
from API.eminfra.graph import GraphService
from API.eminfra.kenmerken import KenmerkService
from API.eminfra.locatie import LocatieService
from API.eminfra.postits import PostitService
from API.eminfra.relaties import RelatieService
from API.eminfra.schadebeheerder import SchadebeheerderService
from API.eminfra.toezichter import ToezichterService

from API.EMInfraDomain import (OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, PagingModeEnum, QueryDTO, BestekRef, \
    BestekKoppeling, FeedPage, AssettypeDTO, AssetDTO, BetrokkenerelatieDTO, AgentDTO, \
    PostitDTO, BestekCategorieEnum, BestekKoppelingStatusEnum, AssetDocumentDTO, LocatieKenmerk, \
    LogicalOpEnum, ToezichterKenmerk, IdentiteitKenmerk, AssetTypeKenmerkTypeDTO, KenmerkTypeDTO, \
    AssetTypeKenmerkTypeAddDTO, ResourceRefDTO, Eigenschap, Event, EventType, EventContext, ExpansionsDTO, \
    RelatieTypeDTO, KenmerkType, EigenschapValueDTO, BeheerobjectDTO, ToezichtgroepTypeEnum, \
    ToezichtgroepDTO, BaseDataclass, BeheerobjectTypeDTO, BoomstructuurAssetTypeEnum, KenmerkTypeEnum, \
    AssetDTOToestand, EigenschapValueUpdateDTO, GeometryNiveau, GeometryBron, GeometryNauwkeurigheid, \
    GeometrieKenmerk, SchadebeheerderKenmerk, AssetRelatieDTO, RelatieEnum, Graph)
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory
from utils.date_helpers import validate_dates, format_datetime
from utils.query_dto_helpers import add_expression


class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings_path: Path = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings_path=settings_path,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

        # Sub-services
        self.assets = AssetService(self.requester)
        self.assettypes = AssettypesService(self.requester)
        self.beheerobject = BeheerobjectService(self.requester)
        self.bestekken = BestekService(self.requester)
        self.documenten = DocumentService(self.requester)
        self.events = EventService(self.requester)
        self.feed = FeedService(self.requester)
        self.geometrie = GeometrieService(self.requester)
        self.graph = GraphService(self.requester)
        self.kenmerken = KenmerkService(self.requester)
        self.locatie = LocatieService(self.requester)
        self.postit = PostitService(self.requester)
        self.relatie = RelatieService(self.requester)
        self.schadebeheerder = SchadebeheerderService(self.requester)
        self.toezichter = ToezichterService(self.requester)

    def search_identiteit(self, naam: str) -> Generator[IdentiteitKenmerk]:
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True,
                                                        logicalOp=None)]
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
                        , TermDTO(property='voornaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}',
                                  logicalOp=LogicalOpEnum.OR)
                        , TermDTO(property='roepnaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}',
                                  logicalOp=LogicalOpEnum.OR)
                        , TermDTO(property='gebruikersnaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}',
                                  logicalOp=LogicalOpEnum.OR)
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

    def get_assets_by_filter(self, filter: dict, size: int = 100, order_by_property: str = None) -> Generator[dict]:
        """filter for otl/assets/search"""
        yield from self.get_objects_from_oslo_search_endpoint(url_part='assets', filter_dict=filter, size=size)

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
            },
            "geldigheid": {
                "van": None,
                "tot": None
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

        response = self.requester.patch(url=f'core/api/assets/{assetId}/kenmerken/{kenmerk_uuid}/eigenschapwaarden',
                                        json=request_body)
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


    def add_relatie(self, assetId: str, kenmerkTypeId: str, relatieTypeId: str, doel_assetId: str) -> None:
        """
            https://apps.mow.vlaanderen.be/eminfra/core/swagger-ui/#/kenmerk/addRelatie_40

        :param assetId:
        :param kenmerkTypeId: 91d6223c-c5d7-4917-9093-f9dc8c68dd3e
        :param relatieTypeId: f2c5c4a1-0899-4053-b3b3-2d662c717b44
        :return:
        """
        _target_asset = self.get_asset_by_id(asset_id=doel_assetId)
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

    def create_assetrelatie(self, bronAsset: AssetDTO, doelAsset: AssetDTO, relatie: RelatieEnum) -> AssetRelatieDTO:
        _, relatietype_id = self.relatie.get_kenmerktype_and_relatietype_id(relatie=relatie)
        json_body = {
            "bronAsset": {
                "uuid": f"{bronAsset.uuid}",
                "_type": f"{bronAsset._type}"
            },
            "doelAsset": {
                "uuid": f"{doelAsset.uuid}",
                "_type": f"{doelAsset._type}"
            },
            "relatieType": {
                "uuid": f"{relatietype_id}"
            }
        }
        url = 'core/api/assetrelaties'
        response = self.requester.post(url=url, json=json_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return self.get_assetrelaties(response.json().get("uuid"))

    def search_assetrelaties(self, type: str, bronAsset: AssetDTO, doelAsset: AssetDTO) -> [AssetRelatieDTO]:
        query_dto = QueryDTO(
            size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=type)]),
                    ExpressionDTO(terms=[TermDTO(property='bronAsset', operator=OperatorEnum.EQ, value=bronAsset.uuid)], logicalOp=LogicalOpEnum.AND),
                    ExpressionDTO(terms=[TermDTO(property='doelAsset', operator=OperatorEnum.EQ, value=doelAsset.uuid)], logicalOp=LogicalOpEnum.AND)
                ]))
        url = 'core/api/assetrelaties/search'
        response = self.requester.post(url=url, data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return [AssetRelatieDTO.from_dict(item) for item in response.json()['data']]

    def get_assetrelaties(self, id: str) -> AssetRelatieDTO:
        """
        Get AssetRelatieDTO object from assetrelatie_uuid (id)
        :param id: asssetrelatie_uuid
        :return: AssetRelatieDTO
        """
        url = f'core/api/assetrelaties/{id}'
        response = self.requester.get(url=url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return AssetRelatieDTO.from_dict(response.json())

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

    def get_eigenschappen(self, assetId: str, eigenschap_naam: str = None) -> list[EigenschapValueDTO]:
        # ophalen kenmerk_uuid
        kenmerken = self.get_kenmerken(assetId=assetId)
        kenmerk_uuid = \
            [kenmerk.type.get('uuid') for kenmerk in kenmerken if kenmerk.type.get('naam').startswith('Eigenschappen')][0]

        # ophalen alle eigenschapwaarden
        url = f'core/api/assets/{assetId}/kenmerken/{kenmerk_uuid}/eigenschapwaarden'
        json_dict = self.requester.get(url).json()
        eigenschappen = [EigenschapValueDTO.from_dict(item) for item in json_dict['data']]

        # optioneel eigenschap waarden filteren
        if eigenschap_naam:
            eigenschappen = [eig for eig in eigenschappen if eig.eigenschap.naam == eigenschap_naam]
        return eigenschappen

    def get_eigenschapwaarden(self, assetId: str, eigenschap_naam: str = None) -> list[EigenschapValueDTO]:
        url = f'core/api/assets/{assetId}/kenmerken/753c1268-68c2-4e67-a6cc-62c0622b576b/eigenschapwaarden'
        json_dict = self.requester.get(url).json()
        eigenschap_value_list = [EigenschapValueDTO.from_dict(item) for item in json_dict['data']]
        if eigenschap_naam:
            eigenschap_value_list = [item for item in eigenschap_value_list if item.eigenschap.naam == eigenschap_naam]
        return eigenschap_value_list

    def update_asset(self, uuid: str, naam: str, toestand: str, commentaar: str, actief: bool):
        """
        Activate asset by default when updating.
        All parameters are mandatory: uuid, naam, toestand, commentaar, actief.

        :param uuid:
        :param naam:
        :param toestand:
        :param commentaar:
        :param actief: default True
        :return:
        """
        json_body = {
            "naam": naam
            , "toestand": toestand
            , "commentaar": commentaar
            , "actief": actief
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
        relatie_gemigreerd = next(
            (r for r in relaties if r.get('@type') == 'https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar'),
            None)
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

    def search_kenmerk_hoortbij(self, asset_uuid: str, naam: KenmerkTypeEnum = KenmerkTypeEnum.HEEFTBIJHORENDEASSETS) -> [KenmerkType]:
        if naam == KenmerkTypeEnum.HEEFTBIJHORENDEASSETS:
            type_id = '5d58905c-412c-44f8-8872-21519041e391'
        else:
            raise NotImplementedError(f'Parameter "naam" = {naam} not implemented')

        query_dto = QueryDTO(
            size=10,
            from_=0,
            expansions=ExpansionsDTO(fields=[f'kenmerk:{type_id}']),
            pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[ExpressionDTO(
                    terms=[
                        TermDTO(property='type.id', operator=OperatorEnum.EQ, value=type_id)
                    ])
                ])
        )
        url = f"core/api/assets/{asset_uuid}/kenmerken/search"
        response = self.requester.post(url=url, data=query_dto.json())
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [KenmerkType.from_dict(item) for item in response.json()['data']]

    def search_assets_via_relatie(self, asset_uuid: str, relatie: RelatieEnum) -> [AssetDTO]:
        """
        Returns a list of assets via the relatie.

        :param asset_uuid: bron asset
        :param relatie: relatieTypeEnum
        :return:
        """
        kenmerkType_uuid, relatieType_uuid = self.relatie.get_kenmerktype_and_relatietype_id(relatie=relatie)
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerkType_uuid}/assets-via/{relatieType_uuid}'
        resp = self.requester.get(url=url)
        if resp.status_code != 200:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())

        return [AssetDTO.from_dict(item) for item in resp.json()['data']]

    def remove_assets_via_relatie(self, bronasset_uuid: str, doelasset_uuid: str, relatie: RelatieEnum) -> None:
        """
        Loskoppelen van een relatie tussen een bron en een doel-asset.

        :param bronasset_uuid: bron asset uuid
        :param doelasset_uuid: doel asset uuid
        :param relatie: relatieTypeEnum
        :return: None

        """
        kenmerkType_uuid, relatieType_uuid = self.relatie.get_kenmerktype_and_relatietype_id(relatie=relatie)
        url = f'core/api/assets/{bronasset_uuid}/kenmerken/{kenmerkType_uuid}/assets-via/{relatieType_uuid}/ops/remove'
        request_body = {
          "name":"remove",
          "description":"Relatie loskoppelen van 1 asset",
          "async":False,
          "uuids": [f"{doelasset_uuid}"]
        }
        resp = self.requester.put(url=url, json=request_body)
        if resp.status_code != 202:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())