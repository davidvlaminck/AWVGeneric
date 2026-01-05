from API.eminfra.EMInfraDomain import (BeheerobjectDTO, BeheerobjectTypeDTO, OperatorEnum, Generator, PagingModeEnum,
                                       QueryDTO, SelectionDTO, ExpressionDTO, TermDTO, LogicalOpEnum, AssetDTO,
                                       BoomstructuurAssetTypeEnum)


class BeheerobjectService:
    def __init__(self, requester):
        self.requester = requester

    def get_beheerobject(self, beheerobject_uuid: str) -> BeheerobjectDTO:
        url = f"core/api/beheerobjecten/{beheerobject_uuid}"
        json_dict = self.requester.get(url).json()
        return BeheerobjectDTO.from_dict(json_dict)

    def search_beheerobjecten_gen(self, naam: str, beheerobjecttype: BeheerobjectTypeDTO = None, actief: bool = None,
                              operator: OperatorEnum = OperatorEnum.CONTAINS) -> Generator[BeheerobjectDTO]:
        query_dto = QueryDTO(
            size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[ExpressionDTO(
                    terms=[TermDTO(property='naam', operator=operator, value=naam)])]))

        if beheerobjecttype:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    logicalOp=LogicalOpEnum.AND
                    , terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=beheerobjecttype.uuid)])
            )

        if actief is not None:
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
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def wijzig_boomstructuur_by_uuid(self, child_asset_uuid: str, parent_asset_uuid: str,
                             parentType: BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict:
        """
        Assets verplaatsen in de boomstructuur met 1 parent en 1 child-asset.

        :param child_asset_uuid:
        :param parent_asset_uuid:
        :return:
        """
        json_body = {
            "name": "reorganize",
            "moveOperations":
                [{"assetsUuids": [f'{child_asset_uuid}']
                     , "targetType": parentType.value
                     , "targetUuid": f'{parent_asset_uuid}'}]
        }
        response = self.requester.put(
            url='core/api/beheerobjecten/ops/reorganize'
            , json=json_body
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))

    def wijzig_boomstructuur(self, childAsset: AssetDTO, parentAsset: AssetDTO,
                             parentType: BoomstructuurAssetTypeEnum = BoomstructuurAssetTypeEnum.ASSET) -> dict:
        """
        Assets verplaatsen in de boomstructuur met 1 parent en 1 child-asset.

        :param childAsset:
        :param parentAsset:
        :return:
        """
        return self.wijzig_boomstructuur_by_uuid(child_asset_uuid=childAsset.uuid, parent_asset_uuid=parentAsset.uuid,
                                                 parentType=parentType)

    def update_beheerobject_status(self, beheerObject: BeheerobjectDTO, status: bool) -> dict:
        json_body = {
            "naam": beheerObject.naam,
            "actief": status,
            "typeUuid": beheerObject.type.get("uuid")
        }
        response = self.requester.put(
            url=f'core/api/beheerobjecten/{beheerObject.uuid}'
            , json=json_body
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json()

    def remove_asset_from_parent_by_uuid(self, asset_uuid: str, parent_asset_uuid: str) -> dict:
        """
        Remove an asset from its parent.
        Wordt gebruikt om een asset uit een boomstructuur te halen, bijvoorbeeld bij OTL-assets.

        :param asset_uuid: Asset uuid to remove from a tree
        :type asset_uuid: str
        :param parent_asset_uuid: Parent asset uuid
        :type parent_asset_uuid: str
        :return: dict
        """
        payload = {
            "name": "remove",
            "description": "Verwijderen uit boomstructuur van 1 asset",
            "async": False,
            "uuids": [asset_uuid],
        }
        url = f"core/api/beheerobjecten/{parent_asset_uuid}/assets/ops/remove"
        response = self.requester.put(
            url=url,
            json=payload
        )
        if response.status_code != 202:
            raise ProcessLookupError(f'Failed to remove parent from asset: {response.text}')
        return response.json()

    def remove_asset_from_parent(self, asset: AssetDTO, parent_asset: AssetDTO) -> dict:
        """
        Remove an asset from its parent.
        Wordt gebruikt om een asset uit een boomstructuur te halen, bijvoorbeeld bij OTL-assets.

        :param asset: Asset to remove from a tree
        :type asset: AssetDTO
        :param parent_asset: Parent asset.
        :type parent_asset: AssetDTO
        :return: dict
        """
        return self.remove_asset_from_parent_by_uuid(asset_uuid=asset.uuid, parent_asset_uuid=parent_asset.uuid)