from collections.abc import Generator

from API.eminfra.EMInfraDomain import (AssetDTO, RelatieTypeDTO, RelatieEnum, AssetRelatieDTO, QueryDTO,
                                       PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum,
                                       LogicalOpEnum)
from API.eminfra.AssetService import AssetService
from API.eminfra.Generic import get_kenmerktype_and_relatietype_id


class RelatieService:
    def __init__(self, requester):
        self.requester = requester

    def search_relaties_generator(self, asset_uuid: str, kenmerktype_id: str, relatietype_id: str) -> Generator[
        RelatieTypeDTO]:
        url = f"core/api/assets/{asset_uuid}/kenmerken/{kenmerktype_id}/assets-via/{relatietype_id}"
        json_dict = self.requester.get(url).json()
        yield from [RelatieTypeDTO.from_dict(item) for item in json_dict['data']]

    def create_assetrelatie(self, bron_asset: AssetDTO, doel_asset: AssetDTO, relatie: RelatieEnum) -> AssetRelatieDTO:
        """
        Maak een assetrelatie op basis van een bron- en een doel-asset

        :param bron_asset: Bron Asset
        :type bron_asset: AssetDTO
        :param doel_asset: Doel Asset
        :type doel_asset: AssetDTO
        :param relatie: Relatie type
        :type relatie: RelatieEnum
        :return AssetRelatieDTO
        """
        _, relatietype_id = get_kenmerktype_and_relatietype_id(relatie=relatie)
        json_body = {
            "bronAsset": {
                "uuid": f"{bron_asset.uuid}",
                "_type": f"{bron_asset._type}"
            },
            "doelAsset": {
                "uuid": f"{doel_asset.uuid}",
                "_type": f"{doel_asset._type}"
            },
            "relatieType": {
                "uuid": f"{relatietype_id}"
            }
        }
        url = 'core/api/assetrelaties'
        response = self.requester.post(url=url, json=json_body)
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return self.get_assetrelatie(response.json().get("uuid"))

    def get_assetrelatie(self, assetrelatie_uuid: str) -> AssetRelatieDTO:
        """
        Get AssetRelatieDTO object from assetrelatie_uuid (id)

        :param assetrelatie_uuid: Asssetrelatie UUID
        :type assetrelatie_uuid: str
        :return: AssetRelatieDTO
        """
        url = f'core/api/assetrelaties/{assetrelatie_uuid}'
        response = self.requester.get(url=url)
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return AssetRelatieDTO.from_dict(response.json())

    def search_assetrelaties(self, bron_asset_uuid: str, doel_asset_uuid: str, relatie: RelatieEnum = None) -> [
        AssetRelatieDTO]:
        """
        Search assetrelaties between two assets

        :param bron_asset_uuid:
        :param doel_asset_uuid:
        :param relatie: RelatieEnum Relatietype
        """
        query_dto = QueryDTO(
            size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(
                        terms=[TermDTO(property='bronAsset', operator=OperatorEnum.EQ, value=bron_asset_uuid)]),
                    ExpressionDTO(terms=[TermDTO(property='doelAsset', operator=OperatorEnum.EQ, value=doel_asset_uuid)],
                                  logicalOp=LogicalOpEnum.AND)
                ]))
        if relatie:
            _, relatietype_uuid = get_kenmerktype_and_relatietype_id(relatie=relatie)
            expression = ExpressionDTO(
                terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=relatietype_uuid)],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)
        url = 'core/api/assetrelaties/search'
        response = self.requester.post(url=url, data=query_dto.json())
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return [AssetRelatieDTO.from_dict(item) for item in response.json()['data']]

    def search_assetrelatie_otl(self, bron_asset_uuid: str = None, doel_asset_uuid: str = None) -> dict:
        if bron_asset_uuid is None and doel_asset_uuid is None:
            raise ValueError('At least one optional parameter "bronAsset" or "doelAsset" must be provided.')
        json_body = {"filters": {}}
        if bron_asset_uuid:
            json_body["filters"]["bronAsset"] = bron_asset_uuid
        if doel_asset_uuid:
            json_body["filters"]["doelAsset"] = doel_asset_uuid
        url = 'core/api/otl/assetrelaties/search'
        response = self.requester.post(url=url, json=json_body)
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response.json().get("@graph")

    def search_assets_via_relatie(self, asset_uuid: str, relatie: RelatieEnum) -> [AssetDTO]:
        """
        Returns a list of assets via the relatie.

        :param asset_uuid: bron asset
        :param relatie: relatieTypeEnum
        :return:
        """
        kenmerktype_id, relatietype_id = get_kenmerktype_and_relatietype_id(relatie=relatie)
        url = f'core/api/assets/{asset_uuid}/kenmerken/{kenmerktype_id}/assets-via/{relatietype_id}'
        resp = self.requester.get(url=url)
        if resp.status_code != 200:
            raise ProcessLookupError(resp.content.decode())
        return [AssetDTO.from_dict(item) for item in resp.json()['data']]

    def remove_relatie(self, bron_asset_uuid: str, doel_asset_uuid: str, relatie: RelatieEnum) -> None:
        """
        Loskoppelen van een relatie tussen een bron en een doel-asset.

        :param bron_asset_uuid: bron asset
        :param doel_asset_uuid: doel asset
        :param relatie: relatieTypeEnum
        :return: None
        """
        kenmerktype_id, relatietype_id = get_kenmerktype_and_relatietype_id(relatie=relatie)
        url = f'core/api/assets/{bron_asset_uuid}/kenmerken/{kenmerktype_id}/assets-via/{relatietype_id}/ops/remove'
        request_body = {
            "name": "remove",
            "description": "Relatie loskoppelen van 1 asset",
            "async": False,
            "uuids": [f"{doel_asset_uuid}"]
        }
        resp = self.requester.put(url=url, json=request_body)
        if resp.status_code != 202:
            raise ProcessLookupError(resp.content.decode())

    def zoek_verweven_asset(self, bron_asset_uuid: str) -> AssetDTO | None:
        """
        Zoek de OTL-asset op basis van een Legacy-asset die verbonden zijn via een Gemigreerd-relatie.
        Returns None indien de Gemigreerd-relatie ontbreekt.

        :param bron_asset_uuid: uuid van de bron asset (Legacy)
        :return:
        """
        relaties = self.search_assetrelatie_otl(bron_asset_uuid=bron_asset_uuid)
        relatie_gemigreerd = next(
            (r for r in relaties if r.get('@type') == 'https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar'),
            None)
        asset_uuid_gemigreerd = relatie_gemigreerd.get('RelatieObject.doelAssetId').get(
            'DtcIdentificator.identificator')[:36]
        return AssetService.get_asset_by_uuid(self, asset_uuid=asset_uuid_gemigreerd)
