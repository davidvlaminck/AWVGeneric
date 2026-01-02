from typing import Generator
from API.EMInfraDomain import (QueryDTO, ExpressionDTO, TermDTO, OperatorEnum, LogicalOpEnum, SelectionDTO,
                               PagingModeEnum, AgentDTO)


class AgentService:
    def __init__(self, requester):
        self.requester = requester

    def search_agent(self, naam: str, ovoCode: str = None, actief: bool = True) -> Generator[AgentDTO]:
        """

        :param naam: agent name
        :type naam: str
        :param ovoCode:
        :type ovoCode: str
        :param actief:
        :type actief: bool
        :return: Generator[AgentDTO]
        :rtype:
        """
        query_dto = QueryDTO(
            size=100,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            expansions={"fields": ["contactInfo"]},
            selection=SelectionDTO(
                expressions=[
                    ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=naam)])
                ]
            )
        )
        if ovoCode:
            query_dto.selection.expressions.append(
                ExpressionDTO(terms=[TermDTO(property='ovoCode', operator=OperatorEnum.EQ, value=ovoCode)]
                              , logicalOp=LogicalOpEnum.AND)
            )
        if actief is not None:
            query_dto.selection.expressions.append(
                ExpressionDTO(terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=actief)]
                              , logicalOp=LogicalOpEnum.AND)
            )
        url = "core/api/agents/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [AgentDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break
