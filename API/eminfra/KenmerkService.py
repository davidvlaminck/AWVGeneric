import logging

from API.eminfra.EMInfraDomain import (AssetTypeKenmerkTypeDTO, KenmerkTypeDTO, PagingModeEnum, SelectionDTO,
                                       ExpressionDTO, TermDTO, QueryDTO, OperatorEnum, ResourceRefDTO,
                                       AssetTypeKenmerkTypeAddDTO, KenmerkTypeEnum, KenmerkType,
                                       AssetDTO)


class KenmerkService:
    def __init__(self, requester):
        self.requester = requester

    def get(self, asset: AssetDTO, kenmerk_uuid: str) -> dict:
        url = f'core/api/assets/{asset.uuid}/kenmerken/{kenmerk_uuid}'
        resp = self.requester.get(url=url)
        if resp.status_code != 200:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())
        return resp.json()

    def put(self, asset: AssetDTO, kenmerk_uuid: str, payload: dict) -> None:
        url = f'core/api/assets/{asset.uuid}/kenmerken/{kenmerk_uuid}'
        resp = self.requester.put(url=url, json=payload)
        if resp.status_code != 202:
            logging.error(resp)
            raise ProcessLookupError(resp.content.decode())

    def get_kenmerktype_by_uuid(self, assettype_uuid: str) -> [AssetTypeKenmerkTypeDTO]:
        """
        Returns a list of kenmerktypes of an assettype

        :param assettype_uuid: assettype uuid
        :type assettype_uuid: str
        :return: [AssetTypeKenmerkTypeDTO]
        :rtype:
        """
        url = f"core/api/assettypes/{assettype_uuid}/kenmerktypes"
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
        add_dto = AssetTypeKenmerkTypeAddDTO(kenmerkType=ResourceRefDTO(uuid=kenmerktype_uuid))
        response = self.requester.post(f'core/api/assettypes/{assettype_uuid}/kenmerktypes', data=add_dto.json())
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_kenmerk_by_uuid(self, asset_uuid: str, kenmerk_uuid: str, request_body: dict) -> None:
        """
        Update een kenmerk van een asset

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param kenmerk_uuid:
        :type kenmerk_uuid: str
        :param request_body:
        :type request_body: dict
        :return: None
        :rtype:
        """
        response = self.requester.put(url=f'core/api/assets/{asset_uuid}/kenmerken/{kenmerk_uuid}', json=request_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_kenmerk(self, asset: AssetDTO, kenmerk_uuid: str, request_body: dict) -> None:
        """
        Update een kenmerk van een asset

        :param asset:
        :type asset: AssetDTO
        :param kenmerk_uuid:
        :type kenmerk_uuid: str
        :param request_body:
        :type request_body: dict
        :return: None
        :rtype:
        """
        return self.update_kenmerk_by_uuid(asset_uuid=asset.uuid, kenmerk_uuid=kenmerk_uuid, request_body=request_body)

    def get_kenmerken_by_uuid(self, asset_uuid: str, naam: KenmerkTypeEnum = None) -> list[KenmerkType] | KenmerkType | None:
        """
        Oplijsten van een specifiek of alle kenmerken van een asset

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param naam: Kenmerk naam. Optioneel.
        :type naam: KenmerkTypeEnum
        :return: [KenmerkType]
        :rtype: list
        """
        url = f'core/api/assets/{asset_uuid}/kenmerken'
        response = self.requester.get(url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        all_kenmerken = [KenmerkType.from_dict(item) for item in response.json()['data']]
        if naam:
            all_kenmerken = [item for item in all_kenmerken if item.type.get("naam").startswith(naam.value)][0]
        return all_kenmerken

    def get_kenmerken(self, asset: AssetDTO, naam: KenmerkTypeEnum = None) -> list[KenmerkType] | KenmerkType:
        """
        Oplijsten van een specifiek of alle kenmerken van een asset

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param naam: Kenmerk naam. Optioneel.
        :type naam: KenmerkTypeEnum
        :return: [KenmerkType]
        :rtype: list
        """
        return self.get_kenmerken_by_uuid(asset_uuid=asset.uuid, naam=naam)

    def get_kenmerk_hoortbij_by_uuid(self, asset_uuid: str) -> [KenmerkType] | None:
        return self.get_kenmerken_by_uuid(asset_uuid=asset_uuid, naam=KenmerkTypeEnum.HEEFTBIJHORENDEASSETS)