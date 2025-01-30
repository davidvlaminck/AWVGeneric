import json
from collections.abc import Generator
from datetime import datetime, timedelta

from pathlib import Path

from API.EMInfraDomain import OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, PagingModeEnum, QueryDTO, BestekRef, \
    BestekKoppeling, FeedPage, AssettypeDTO, AssettypeDTOList, DTOList, AssetDTO, BestekCategorieEnum, \
    BestekKoppelingStatusEnum
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory
from utils.date_helpers import validate_dates, format_datetime


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

        return [BestekRef.from_dict(item) for item in response.json()['data']]

    def get_bestekref_by_eDelta_besteknummer(self, eDelta_besteknummer: str) -> [BestekRef]:
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[TermDTO(property='eDeltaBesteknummer',
                                                    operator=OperatorEnum.EQ,
                                                    value=eDelta_besteknummer)])]))

        response = self.requester.post('core/api/bestekrefs/search', data=query_dto.json())
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [BestekRef.from_dict(item) for item in response.json()['data']]


    def change_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str, bestekkoppelingen: [BestekKoppeling]) -> None:
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken',
            data=json.dumps({'data': [item.asdict() for item in bestekkoppelingen]}))
        if response.status_code != 202:
            print(response)
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

    def add_bestekkoppeling(self, asset_uuid: str, eDelta_besteknummer: str = None, eDelta_dossiernummer: str = None, start_datetime: datetime = datetime.now(), end_datetime: datetime = None, categorie: str = BestekCategorieEnum.WERKBESTEK) -> dict | None:
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
            new_bestekRef = self.get_bestekref_by_eDelta_besteknummer(eDelta_besteknummer=eDelta_besteknummer)[0]
        else:
            new_bestekRef = self.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)[0]

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
            bestekkoppelingen.insert(0, new_bestekkoppeling)

            return self.change_bestekkoppelingen_by_asset_uuid(asset_uuid, bestekkoppelingen)

    def replace_bestekkoppeling(self, asset_uuid: str, eDelta_besteknummer_old: str = None, eDelta_dossiernummer_old: str = None, eDelta_besteknummer_new: str = None, eDelta_dossiernummer_new: str = None, start_datetime: datetime = datetime.now(), end_datetime: datetime = None, categorie: str = BestekCategorieEnum.WERKBESTEK) -> dict | None:
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
            bestekref = self.get_bestekref_by_eDelta_besteknummer(eDelta_besteknummer=eDelta_besteknummer_old)[0]
        else:
            bestekref = self.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer_old)[0]

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
