from typing import Generator

from API.eminfra.EMInfraDomain import (AssetDTO, RelatieTypeDTO, RelatieEnum, AssetRelatieDTO, QueryDTO,
                                       PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum,
                                       LogicalOpEnum)
from API.eminfra.AssetService import AssetService


class RelatieService:
    def __init__(self, requester):
        self.requester = requester

    def search_relaties_generator(self, asset_uuid: str, kenmerktype_id: str, relatietype_id: str) -> Generator[
        RelatieTypeDTO]:
        url = f"core/api/assets/{asset_uuid}/kenmerken/{kenmerktype_id}/assets-via/{relatietype_id}"
        json_dict = self.requester.get(url).json()
        yield from [RelatieTypeDTO.from_dict(item) for item in json_dict['data']]

    @classmethod
    def get_kenmerktype_and_relatietype_id(cls, relatie: RelatieEnum) -> (str, str):
        """
        Returns kenmerktype_uuid and relatietype_uuid.

        :param relatie: RelatieEnum
        :return: Tuple of strings kenmerktype_uuid and relatietype_uuid
        """
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
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBijhorendeAssets": [
                "5d58905c-412c-44f8-8872-21519041e391",
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
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#GeeftBevestigingAan": [
                "cef6a3c0-fd1b-48c3-8ee0-f723e55dd02b",
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
        return relaties_dict[relatie.value]

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
        _, relatietype_id = self.get_kenmerktype_and_relatietype_id(relatie=relatie)
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
            _, relatietype_uuid = self.get_kenmerktype_and_relatietype_id(relatie=relatie)
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
        kenmerktype_id, relatietype_id = self.get_kenmerktype_and_relatietype_id(relatie=relatie)
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
        kenmerktype_id, relatietype_id = self.get_kenmerktype_and_relatietype_id(relatie=relatie)
        url = f'core/api/assets/{bron_asset_uuid.uuid}/kenmerken/{kenmerktype_id}/assets-via/{relatietype_id}/ops/remove'
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
