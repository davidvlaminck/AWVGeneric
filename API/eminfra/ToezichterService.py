from collections.abc import Generator

from API.eminfra.EMInfraDomain import (AssetDTO, ToezichterKenmerk, IdentiteitKenmerk, ToezichtgroepDTO, QueryDTO,
                                       SelectionDTO, PagingModeEnum, ExpressionDTO, TermDTO, OperatorEnum,
                                       LogicalOpEnum, BetrokkenerelatieDTO, ToezichtgroepTypeEnum,
                                       ToezichtKenmerkUpdateDTO)


class ToezichterService:
    def __init__(self, requester):
        self.requester = requester
        self.TOEZICHTER_UUID = 'f0166ba2-757c-4cf3-bf71-2e4fdff43fa3'

    def get_toezichter_by_uuid(self, asset_uuid: str) -> ToezichterKenmerk:
        response = self.requester.get(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.TOEZICHTER_UUID}')
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return ToezichterKenmerk.from_dict(response.json())

    def get_toezichter(self, asset: AssetDTO) -> ToezichterKenmerk:
        return self.get_toezichter_by_uuid(asset_uuid=asset.uuid)

    def update_toezichtkenmerk(self, asset_uuid: str, toezichtkenmerkupdate: ToezichtKenmerkUpdateDTO) -> None:
        """
        Update toezicht kenmerk.

        Updating both toezichter and toezichtgroep.

        :param asset_uuid: Asset UUID
        :type asset_uuid: str
        :param toezichtkenmerkupdate:
        :type toezichtkenmerkupdate: ToezichtKenmerkUpdateDTO
        :return: None
        """
        payload = {}
        if toezichtkenmerkupdate.toezichter:
            payload["toezichter"] = {"uuid": toezichtkenmerkupdate.toezichter.uuid}
        else:
            payload["toezichter"] = None
        if toezichtkenmerkupdate.toezichtGroep:
            payload["toezichtGroep"] = {"uuid": toezichtkenmerkupdate.toezichtGroep.uuid}
        else:
            payload["toezichtGroep"] = None

        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.TOEZICHTER_UUID}',
            json=payload
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))

    def add_toezichter(self, asset_uuid: str, toezichtgroep_uuid: str, toezichter_uuid: str) -> None:
        """
        Deprecated. Use function update_toezichtkenmerk() instead.

        Both toezichter and toezichtsgroep are mandatory.
        Updating only one of both (toezichter/toezichtsgroep), purges the other.

        :param asset_uuid: Asset uuid
        :param toezichtgroep_uuid: Toezichtsgroep uuid
        :param toezichter_uuid: Toezichter uuid
        :return:
        """
        payload = {}
        if toezichter_uuid:
            payload["toezichter"] = {
                "uuid": toezichter_uuid
            }
        if toezichtgroep_uuid:
            payload["toezichtGroep"] = {
                "uuid": toezichtgroep_uuid
            }
        response = self.requester.put(
            url=f'core/api/assets/{asset_uuid}/kenmerken/{self.TOEZICHTER_UUID}',
            json=payload
        )
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_identiteit(self, toezichter_uuid: str) -> IdentiteitKenmerk:
        response = self.requester.get(
            url=f'identiteit/api/identiteiten/{toezichter_uuid}')
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return IdentiteitKenmerk.from_dict(response.json())

    def get_toezichtgroep(self, toezichtgroep_uuid: str) -> ToezichtgroepDTO:
        response = self.requester.get(
            url=f'identiteit/api/toezichtgroepen/{toezichtgroep_uuid}')
        if response.status_code != 200:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return ToezichtgroepDTO.from_dict(response.json())

    def search_toezichtgroep_lgc(self, naam: str, type: ToezichtgroepTypeEnum = None) -> Generator[ToezichtgroepDTO]:
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
                        TermDTO(property='type', operator=OperatorEnum.EQ, value=type)],
                    logicalOp=LogicalOpEnum.AND))
        url = "identiteit/api/toezichtgroepen/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [ToezichtgroepDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_identiteit(self, naam: str) -> Generator[IdentiteitKenmerk]:
        """
        Zoek een toezichter op basis van diens naam. Splits de input op spaties en zoek op ieder deel van de naam.
        """
        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True,
                                                        logicalOp=None)],
                                         logicalOp=None
                                     )
                                 ]
                             )
                             )

        naam_parts = naam.split(' ')
        for naam_part in naam_parts:
            query_dto.selection.expressions.append(
                ExpressionDTO(
                    terms=[
                        TermDTO(property='naam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}', logicalOp=None),
                        TermDTO(property='voornaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}',
                                logicalOp=LogicalOpEnum.OR),
                        TermDTO(property='roepnaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}',
                                logicalOp=LogicalOpEnum.OR),
                        TermDTO(property='gebruikersnaam', operator=OperatorEnum.CONTAINS, value=f'{naam_part}',
                                logicalOp=LogicalOpEnum.OR)
                    ],
                    logicalOp=LogicalOpEnum.AND
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

    def add_betrokkenerelatie(self, asset: AssetDTO, agent_uuid: str, rol: str) -> BetrokkenerelatieDTO:
        json_body = {
            "bron": {
                "uuid": f"{asset.uuid}",
                "_type": f"{asset._type}"
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
            raise ProcessLookupError(response.content.decode("utf-8"))
        return BetrokkenerelatieDTO.from_dict(response.json())

    def remove_betrokkenerelatie(self, betrokkenerelatie_uuid: str) -> dict:
        url = f"core/api/betrokkenerelaties/{betrokkenerelatie_uuid}"
        response = self.requester.delete(url=url)
        if response.status_code != 202:
            raise ProcessLookupError(response.content.decode("utf-8"))
        return response
