import logging
from typing import Generator

from API.eminfra.EMInfraDomain import (Eigenschap, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, QueryDTO,
                                       OperatorEnum, LogicalOpEnum, EigenschapValueDTO, EigenschapValueUpdateDTO,
                                       KenmerkTypeEnum, AssetDTO)
from API.eminfra.KenmerkService import KenmerkService

class EigenschapService:
    def __init__(self, requester):
        self.requester = requester


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

    def update_eigenschap(self, asset: AssetDTO, eigenschap: EigenschapValueDTO | EigenschapValueUpdateDTO) -> None:
        """
        Updates an eigenschap value on an asset, handling both DTO types.

        :param asset: Asset
        :type asset: AssetDTO
        :param eigenschap
        :type eigenschap: EigenschapValueDTO | EigenschapValueUpdateDTO
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
            kenmerk_eigenschap = KenmerkService.get_kenmerken(assetId=asset.uuid, naam=KenmerkTypeEnum.EIGENSCHAPPEN)
            kenmerk_uuid = kenmerk_eigenschap.type.get("uuid", None)

        response = self.requester.patch(url=f'core/api/assets/{asset.uuid}/kenmerken/{kenmerk_uuid}/eigenschapwaarden',
                                        json=request_body)
        if response.status_code != 202:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def list_eigenschap(self, kenmerktypeId: str) -> [Eigenschap]:
        url = f"core/api/kenmerktypes/{kenmerktypeId}/eigenschappen"
        json_dict = self.requester.get(url).json()
        return [Eigenschap.from_dict(item) for item in json_dict['data']]


    def get_eigenschappen(self, assetId: str, eigenschap_naam: str = None) -> list[EigenschapValueDTO]:
        # ophalen kenmerk_uuid
        kenmerken = KenmerkService.get_kenmerken(self, assetId=assetId)
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



    def search_eigenschapwaarden(self, assetId: str) -> [EigenschapValueDTO]:
        url = f'core/api/assets/{assetId}/kenmerken/cef6a3c0-fd1b-48c3-8ee0-f723e55dd02b/eigenschapwaarden'
        response = self.requester.post(url=url)
        if response.status_code != 200:
            logging.error(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        return [EigenschapValueDTO.from_dict(item) for item in response.json()['data']]

    def get_eigenschapwaarden(self, assetId: str, eigenschap_naam: str = None) -> list[EigenschapValueDTO]:
        url = f'core/api/assets/{assetId}/kenmerken/753c1268-68c2-4e67-a6cc-62c0622b576b/eigenschapwaarden'
        json_dict = self.requester.get(url).json()
        eigenschap_value_list = [EigenschapValueDTO.from_dict(item) for item in json_dict['data']]
        if eigenschap_naam:
            eigenschap_value_list = [item for item in eigenschap_value_list if item.eigenschap.naam == eigenschap_naam]
        return eigenschap_value_list