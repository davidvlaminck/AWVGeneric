import logging

from API.EMInfraDomain import AssetTypeKenmerkTypeDTO, KenmerkTypeDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, \
    TermDTO, QueryDTO, OperatorEnum, ResourceRefDTO, AssetTypeKenmerkTypeAddDTO, KenmerkTypeEnum, KenmerkType, \
    ExpansionsDTO, AssetDTO


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

    def get_kenmerktype_by_assettype_uuid(self, uuid: str) -> [AssetTypeKenmerkTypeDTO]:
        """
        Returns a list of kenmerktypes of an assettype
        :param uuid: assettype uuid
        :type uuid: str
        :return: [AssetTypeKenmerkTypeDTO]
        :rtype:
        """
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
        response = self.requester.put(url=f'core/api/assets/{asset.uuid}/kenmerken/{kenmerk_uuid}', json=request_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_kenmerken(self, assetId: str, naam: KenmerkTypeEnum = None) -> list[KenmerkType] | KenmerkType:
        """
        Oplijsten van een specifiek of alle kenmerken van een asset

        :param assetId:
        :type assetId: AssetDTO
        :param naam: Kenmerk naam. Optioneel.
        :type naam: KenmerkTypeEnum
        :return: [KenmerkType]
        :rtype: list
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