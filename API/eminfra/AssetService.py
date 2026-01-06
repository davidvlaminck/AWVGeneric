import json
from typing import Generator
from API.eminfra.EMInfraDomain import (AssetDTO, AssetDTOToestand, QueryDTO, ExpressionDTO, TermDTO, OperatorEnum,
                                       LogicalOpEnum, ExpansionsDTO, SelectionDTO, PagingModeEnum, AssettypeDTO, RelatieEnum,
                                       BoomstructuurAssetTypeEnum)
from API.eminfra.RelatieService import RelatieService


class AssetService:
    def __init__(self, requester):
        self.requester = requester

    def get_asset_by_uuid(self, asset_uuid: str) -> AssetDTO:
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

    def update_asset_by_uuid(self, asset_uuid: str, naam: str = None, actief: bool = None, toestand: AssetDTOToestand = None,
                     commentaar: str = None) -> dict:
        asset = self.get_asset_by_uuid(asset_uuid=asset_uuid)
        return self._update_asset(asset=asset, naam=naam, actief=actief, toestand=toestand, commentaar=commentaar)

    def update_asset(self, asset: AssetDTO, naam: str = None, actief: bool = None, toestand: AssetDTOToestand = None,
                     commentaar: str = None) -> dict:
        return self._update_asset(asset=asset, naam=naam, actief=actief, toestand=toestand, commentaar=commentaar)

    def update_toestand_by_uuid(self, asset_uuid: str, toestand: AssetDTOToestand = AssetDTOToestand.IN_ONTWERP) -> dict:
        """
        Update toestand of an asset.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param toestand:
        :type toestand: AssetDTOToestand
        :return:
        """
        asset = self.get_asset_by_uuid(asset_uuid=asset_uuid)
        return self._update_asset(asset=asset, toestand=toestand)

    def update_toestand(self, asset: AssetDTO, toestand: AssetDTOToestand = AssetDTOToestand.IN_ONTWERP) -> dict:
        """
        Update toestand of an asset.

        :param asset: Asset
        :type asset: AssetDTO
        :param toestand:
        :type toestand: AssetDTOToestand
        :return:
        """
        return self._update_asset(asset=asset, toestand=toestand)

    def update_commentaar_by_uuid(self, asset_uuid: str, commentaar: str) -> dict:
        """
        Update commentaar of an asset.

        :param asset_uuid: Asset uuid
        :type asset: str
        :param commentaar: nieuwe commentaar
        :type commentaar: str
        :return:
        :rtype:
        """
        asset = self.get_asset_by_uuid(asset_uuid=asset_uuid)
        return self._update_asset(asset=asset, commentaar=commentaar)

    def update_commentaar(self, asset: AssetDTO, commentaar: str) -> dict:
        """
        Update commentaar of an asset.

        :param asset: Asset
        :type asset: AssetDTO
        :param commentaar: nieuwe commentaar
        :type commentaar: str
        :return:
        :rtype:
        """
        return self._update_asset(asset=asset, commentaar=commentaar)

    def activeer_asset_by_uuid(self, asset_uuid: str) -> dict:
        asset = self.get_asset_by_uuid(asset_uuid=asset_uuid)
        return self._update_asset(asset=asset, actief=True)

    def activeer_asset(self, asset: AssetDTO) -> dict:
        return self._update_asset(asset=asset, actief=True)

    def deactiveer_asset_by_uuid(self, asset_uuid: str) -> dict:
        asset_uuid = self.get_asset_by_uuid(asset_uuid=asset_uuid)
        return self._update_asset(asset=asset_uuid, actief=False)

    def deactiveer_asset(self, asset: AssetDTO) -> dict:
        return self._update_asset(asset=asset, actief=False)

    def _search_assets_helper_generator(self, query_dto: QueryDTO) -> Generator[AssetDTO]:
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

    def search_assets_generator(self, query_dto: QueryDTO, actief: bool = None) -> Generator[AssetDTO]:
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
        yield from self._search_assets_helper_generator(query_dto)

    def search_asset_by_name_generator(self, asset_name: str, exact_search: bool = True) -> Generator[AssetDTO]:
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
        yield from self._search_assets_helper_generator(query_dto)

    def search_child_assets_by_uuid_generator(self, asset_uuid: str, recursive: bool = False) -> Generator[AssetDTO] | None:
        """
        Zoek actieve child-assets in een boomstructuur uit EM-infra.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
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
        url = f"core/api/assets/{asset_uuid}/assets/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            assets = [AssetDTO.from_dict(item) for item in json_dict['data']]
            for asset in assets:
                yield asset  # yield the current asset
                # If recursive, call recursively for each asset's uuid
                if recursive:
                    yield from self.search_child_assets_generator(asset=asset, recursive=True)
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_child_assets_generator(self, asset: AssetDTO, recursive: bool = False) -> Generator[AssetDTO] | None:
        """
        Zoek actieve child-assets in een boomstructuur uit EM-infra.
        
        :param asset: Asset
        :type asset: AssetDTO
        :param recursive: recursive search in tree structure
        :type recursive: bool
        :return: Generator[AssetDTO]
        """
        return self.search_child_assets_by_uuid_generator(asset_uuid=asset.uuid, recursive=recursive)

    def search_parent_asset_by_uuid(self, asset_uuid: str, recursive: bool = False,
                            return_all_parents: bool = False) -> AssetDTO | list[AssetDTO] | None:
        """
        Search for the parent asset(s) of a given asset UUID.

        :param asset_uuid: Asset uuid to search the parent for
        :type asset_uuid: str
        :param recursive: If True, search recursively up the parent chain.
        :type recursive: bool
        :param return_all_parents: If True, return a list of all parents; if False, return only the final parent.
        :type return_all_parents: bool
        :return: AssetDTO | list[AssetDTO] | None
        """
        query_dto = QueryDTO(size=1, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent']), selection=SelectionDTO(
                            expressions=[ExpressionDTO( terms=[
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
        return self.search_parent_asset_by_uuid(asset_uuid=asset.uuid, recursive=recursive,
                                                return_all_parents=return_all_parents)

    def create_asset_by_uuid_and_relatie(self, asset_uuid: str, naam: str, assettype: AssettypeDTO, relatie: RelatieEnum) -> dict:
        """
        Maakt zowel een nieuwe asset en tevens een relatie aan vanuit een bestaande asset.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param naam: naam van de nieuw aan te maken asset
        :type naam: str
        :param assettype: assettype van de nieuw aan te maken asset
        :type assettype: AssettypeDTO
        :param relatie: aan te maken relatie
        :type relatie: RelatieEnum
        :return: dict
        """
        kenmerkTypeId, relatieTypeId = RelatieService.get_kenmerktype_and_relatietype_id(relatie)
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerkTypeId}/assets-via/{relatieTypeId}/nieuw'
        request_body = {"naam": naam, "typeUuid": assettype.uuid}
        response = self.requester.post(url=url, json=request_body)
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()


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
        return self.create_asset_by_uuid_and_relatie(asset_uuid=asset.uuid, naam=naam, assettype=assettype, relatie=relatie)


    def create_asset_by_uuid(self, parent_asset_uuid: str, naam: str, assettype: AssettypeDTO,
                     parent_assettype: BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict | None:
        """
        Maak een nieuwe asset aan op een specifieke plaats in de boomstructuur van EM-Infra

        :param parent_asset_uuid: Parent asset uuid waaronder de nieuwe asset dient te worden geplaatst
        :type parent_asset_uuid: str
        :param naam: Naam van de nieuwe asset
        :type naam: str
        :param assettype:  assettype van de nieuwe asset
        :type assettype: AssettypeDTO
        :param parent_assettype:
        :type parent_assettype:
        :return:
        :rtype:
        """
        json_body = {
            "naam": naam,
            "typeUuid": assettype.uuid
        }

        if parent_assettype.value == 'asset':
            prefix = 'assets'
        elif parent_assettype.value == 'beheerobject':
            prefix = 'beheerobjecten'
        else:
            raise ValueError(f"Unexpected parent_asset_type: {parent_assettype.value}")

        url = f'core/api/{prefix}/{parent_asset_uuid}/assets'

        response = self.requester.post(url, json=json_body)
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def create_asset(self, parent_asset: AssetDTO, naam: str, assettype: AssettypeDTO,
                     parent_assettype: BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict | None:
        """
        Maak een nieuwe asset aan op een specifieke plaats in de boomstructuur van EM-Infra

        :param parent_asset: Parent asset waaronder de nieuwe asset dient te worden geplaatst
        :type parent_asset: AssetDTO
        :param naam: Naam van de nieuwe asset
        :type naam: str
        :param assettype:  assettype van de nieuwe asset
        :type assettype: AssettypeDTO
        :param parent_assettype:
        :type parent_assettype:
        :return:
        :rtype:
        """
        return self.create_asset_by_uuid(parent_asset_uuid=parent_asset.uuid, naam=naam,
                                         assettype=assettype, parent_assettype=parent_assettype)

    def get_assets_by_filter_gen(self, filter: dict, size: int = 100) -> Generator[dict]:
        """filter for otl/assets/search"""
        yield from self.get_objects_from_oslo_search_endpoint_gen(url_part='assets', filter_dict=filter, size=size)

    def get_objects_from_oslo_search_endpoint_gen(self, url_part: str,
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