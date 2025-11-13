from API.EMInfraDomain import ExpressionDTO, TermDTO, QueryDTO, PagingModeEnum, SelectionDTO, OperatorEnum, \
    LogicalOpEnum
from utils.date_helpers import format_datetime


def add_expression(query_dto, property_name, operator, date_value):
    """Helper function to create and append an ExpressionDTO."""
    date_str = format_datetime(date_value)
    expression = ExpressionDTO(terms=[TermDTO(property=property_name, operator=operator, value=date_str)])
    query_dto.selection.expressions.append(expression)
    return query_dto


def build_query_search_dnblaagspanning(eanNummer: str, assettype_uuid: str) -> QueryDTO:
    return QueryDTO(
        size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(
                        property='eig:b924b988-a3b4-4d0c-aa0e-c69ff503e784:a108fc8a-c522-4469-8410-62f5a0241698',
                        operator=OperatorEnum.EQ,
                        value=f"{eanNummer}")]),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='type',
                        operator=OperatorEnum.EQ,
                        value=f"{assettype_uuid}")]
                    , logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='actief',
                        operator=OperatorEnum.EQ,
                        value=True)]
                    , logicalOp=LogicalOpEnum.AND)
            ]))


def build_query_search_energiemeter(energiemeter_naam: str, assettype_uuid: str) -> QueryDTO:
    return QueryDTO(
        size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(
                        property='naam',
                        operator=OperatorEnum.EQ,
                        value=f"{energiemeter_naam}")]),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='type',
                        operator=OperatorEnum.EQ,
                        value=f"{assettype_uuid}")]
                    , logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='actief',
                        operator=OperatorEnum.EQ,
                        value=True)]
                    , logicalOp=LogicalOpEnum.AND)
            ]))
