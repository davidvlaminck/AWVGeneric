import json
from typing import Generator
from API.EMInfraDomain import (AssetDTO, AssetDTOToestand, QueryDTO, ExpressionDTO, TermDTO, OperatorEnum,
                               LogicalOpEnum, ExpansionsDTO, SelectionDTO, PagingModeEnum, AssettypeDTO, RelatieEnum,
                               BoomstructuurAssetTypeEnum)
from API.eminfra.relaties import RelatieService


class AssetService:
    def __init__(self, requester):
        self.requester = requester

    @staticmethod
    def get_asset(self, asset_uuid: str) -> AssetDTO:
        """
        Get the AssetDTO object from an asset_uuid

        :param asset_uuid:
        :type asset_uuid:
        :return: AssetDTO
        :rtype:
        """
        url = f"core/api/assets/{asset_uuid}"
        json_dict = self.requester.get(url).json()
        return AssetDTO.from_dict(json_dict)

    def _update_asset(self, asset: AssetDTO, naam: str = None, actief: bool = None, toestand: AssetDTOToestand = None,
                      commentaar: str = None) -> dict:
        """
        Update an asset.
        All parameters are mandatory. When empty, the actual value is preserved.
        """
        # default bestaande waardes van de Asset.
        json_body = {
            "naam": asset.naam,
            "actief": asset.actief,
            "toestand": asset.toestand.value,
            "commentaar": asset.commentaar
        }
        # update asset eigenschappen naam, actief, toestand en commentaar
        if naam:
            json_body["naam"] = naam
        if actief is not None:
            json_body["actief"] = actief
        if toestand:
            json_body["toestand"] = toestand.value
        if commentaar:
            json_body["commentaar"] = commentaar
        response = self.requester.put(
            url=f'core/api/assets/{asset.uuid}'
            , json=json_body
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def update_asset(self, asset: AssetDTO, naam: str = None, actief: bool = None, toestand: AssetDTOToestand = None,
                     commentaar: str = None) -> dict:
        return self._update_asset(asset=asset, naam=naam, actief=actief, toestand=toestand, commentaar=commentaar)

    def update_toestand(self, asset: AssetDTO, toestand: AssetDTOToestand = AssetDTOToestand.IN_ONTWERP) -> dict:
        """
        Update toestand of an asset.

        :param asset:
        :param toestand:
        :return:
        """
        return self._update_asset(asset=asset, toestand=toestand)

    def update_commentaar(self, asset: AssetDTO, commentaar: str) -> dict:
        """
        Update commentaar of an asset.

        :param asset:
        :type asset: AssetDTO
        :param commentaar: nieuwe commentaar
        :type commentaar: str
        :return:
        :rtype:
        """
        return self._update_asset(asset=asset, commentaar=commentaar)


    def activeer_asset(self, asset: AssetDTO) -> dict:
        return self._update_asset(asset=asset, actief=True)

    def deactiveer_asset(self, asset: AssetDTO) -> dict:
        return self._update_asset(asset=asset, actief=False)

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

    def search_assets(self, query_dto: QueryDTO, actief: bool = None) -> Generator[AssetDTO]:
        """
        Search assets using a query.
        Status actief default None. Set status actief (boolean) to filter active or inactive assets.
        """
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

    @staticmethod
    def search_asset_by_uuid(self, asset_uuid: str) -> Generator[AssetDTO]:
        """
        Search active and inactive assets by uuid.

        :param asset_uuid: asset identificator
        :type asset_uuid: str
        :return: Generator[AssetDTO]
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
        Search active and inactive assets by name.
        Exact_search (default True) searches for an exact match operator EQUALS,
        while exact_search = False loosely searches using operator CONTAINS.

        :param asset_name: asset name
        :type asset_name: str
        :param exact_search: exact search (True) or loose search (False). Defaults True - exact search.
        :type exact_search: bool
        :return: Generator[AssetDTO]
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

    def search_child_assets(self, asset: AssetDTO, recursive: bool = False) -> Generator[AssetDTO] | None:
        """
        Zoek actieve child-assets in een boomstructuur uit EM-infra.
        
        :param asset:
        :type asset: AssetDTO
        :param recursive: recursive search in tree structure
        :type recursive: bool
        :return: Generator[AssetDTO]
        """
        query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent']),
                             selection=SelectionDTO(
                                 expressions=[ExpressionDTO(
                                     terms=[
                                         TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)
                                     ])]))
        url = f"core/api/assets/{asset.uuid}/assets/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            assets = [AssetDTO.from_dict(item) for item in json_dict['data']]
            for asset in assets:
                yield asset  # yield the current asset
                # If recursive, call recursively for each asset's uuid
                if recursive:
                    yield from self.search_child_assets(asset=asset, recursive=True)
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_parent_asset(self, asset: AssetDTO, recursive: bool = False,
                            return_all_parents: bool = False) -> AssetDTO | list[AssetDTO] | None:
        """
        Search for the parent asset(s) of a given asset UUID.

        :param asset: Asset to search the parent for
        :type asset: AssetDTO
        :param recursive: If True, search recursively up the parent chain.
        :type recursive: bool
        :param return_all_parents: If True, return a list of all parents; if False, return only the final parent.
        :type return_all_parents: bool
        :return: AssetDTO | list[AssetDTO] | None
        """
        query_dto = QueryDTO(size=1, from_=0, pagingMode=PagingModeEnum.OFFSET, 
                             expansions=ExpansionsDTO(fields=['parent']), selection=SelectionDTO(
                            expressions=[ExpressionDTO( terms=[
                                    TermDTO(property='id', operator=OperatorEnum.EQ, value=asset.uuid)
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
        next_parent = self.search_parent_asset(parent_asset, recursive=True, return_all_parents=True)

        if next_parent:
            if isinstance(next_parent, list):
                parents.extend(next_parent)
            else:
                parents.append(next_parent)

        if return_all_parents:
            return parents
        else:
            return parents[-1]  # Return only the last parent (the top-most one)

    def create_asset_and_relatie(self, asset: AssetDTO, naam: str, assettype: AssettypeDTO, relatie: RelatieEnum) -> dict:
        """
        Maakt zowel een nieuwe asset en tevens een relatie aan vanuit een bestaande asset.

        :param asset: bron asset van de aan te maken relatie
        :type asset: AssetDTO
        :param naam: naam van de nieuw aan te maken asset
        :type naam: str
        :param assettype: assettype van de nieuw aan te maken asset
        :type assettype: AssettypeDTO
        :param relatie: aan te maken relatie
        :type relatie: RelatieEnum
        :return: dict
        """
        kenmerkTypeId, relatieTypeId = RelatieService.get_kenmerktype_and_relatietype_id(relatie=relatie)
        url = f'core/api/assets/{asset.uuid}/kenmerken/{kenmerkTypeId}/assets-via/{relatieTypeId}/nieuw'
        request_body = {"naam": naam, "typeUuid": assettype.uuid}
        response = self.requester.post(url=url, json=request_body)
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def create_asset(self, parentAsset: AssetDTO, naam: str, assettype: AssettypeDTO,
                     parentAssetType: BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict | None:
        """
        Maak een nieuwe asset aan op een specifieke plaats in de boomstructuur van EM-Infra

        :param parentAsset: Parent asset waaronder de nieuwe asset dient te worden geplaatst
        :type parentAsset: AssetDTO
        :param naam: Naam van de nieuwe asset
        :type naam: str
        :param assettype:  assettype van de nieuwe asset
        :type assettype: AssettypeDTO
        :param parentAssetType:
        :type parentAssetType:
        :return:
        :rtype:
        """
        json_body = {
            "naam": naam,
            "typeUuid": assettype.uuid
        }

        if parentAssetType.value == 'asset':
            prefix = 'assets'
        elif parentAssetType.value == 'beheerobject':
            prefix = 'beheerobjecten'
        else:
            raise ValueError(f"Unexpected parent_asset_type: {parentAssetType.value}")

        url = f'core/api/{prefix}/{parentAsset.uuid}/assets'

        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def get_assets_by_filter(self, filter: dict, size: int = 100) -> Generator[dict]:
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
        body = {'size': size, 'fromCursor': None, 'filters': filter_dict, 'expansion': {"fields": []}}
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