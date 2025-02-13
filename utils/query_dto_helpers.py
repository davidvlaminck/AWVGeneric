from API.EMInfraDomain import ExpressionDTO, TermDTO
from utils.date_helpers import format_datetime


def add_expression(query_dto, property_name, operator, date_value):
    """Helper function to create and append an ExpressionDTO."""
    date_str = format_datetime(date_value)
    expression = ExpressionDTO(terms=[TermDTO(property=property_name, operator=operator, value=date_str)])
    query_dto.selection.expressions.append(expression)
    return query_dto