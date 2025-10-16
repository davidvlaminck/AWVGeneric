import pandas as pd
from pathlib import Path

from API.EMInfraDomain import AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    AgentDTO, LogicalOpEnum


def load_settings(user: str = 'Dries'):
    if user == 'Dries':
        return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    else:
        raise NotImplementedError(f'user: {user} is not implemented in function call load_settings()')

def read_rsa_report(filepath: Path, usecols: [str] = None) -> pd.DataFrame:
    """Read RSA-report as input into a DataFrame."""
    if not usecols:
        usecols = ["uuid"]
    return pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=usecols)

def build_query_search_betrokkenerelaties(bronAsset: AssetDTO = None, agent: AgentDTO = None, rol: str = None) -> QueryDTO:
    if all(par is None for par in (bronAsset, agent, rol)):
        raise ValueError("At least one of the parameters 'bronAsset', 'agent', or 'rol' must be provided.")

    query = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET, selection=SelectionDTO(expressions=[]))

    counter_expressions = 0
    if bronAsset:
        logicalOp = None if counter_expressions == 0 else LogicalOpEnum.AND
        expression_bronAsset = ExpressionDTO(
            terms=[TermDTO(property='bronAsset', operator=OperatorEnum.EQ, value=f'{bronAsset.uuid}')],
            logicalOp=logicalOp)
        query.selection.expressions.append(expression_bronAsset)
        counter_expressions += 1
    if agent:
        logicalOp = None if counter_expressions == 0 else LogicalOpEnum.AND
        expression_agent = ExpressionDTO(
            terms=[TermDTO(property='agent', operator=OperatorEnum.EQ, value=f'{agent.uuid}')], logicalOp=logicalOp)
        query.selection.expressions.append(expression_agent)
        counter_expressions += 1
    if rol:
        logicalOp = None if counter_expressions == 0 else LogicalOpEnum.AND
        expression_rol = ExpressionDTO(terms=[TermDTO(property='rol', operator=OperatorEnum.EQ, value=rol)],
                                         logicalOp=logicalOp)
        query.selection.expressions.append(expression_rol)
        counter_expressions += 1
    return query


def build_query_search_assets(bronAsset: AssetDTO = None, agent: AgentDTO = None, rol: str = None,
                                          actief: bool = True) -> QueryDTO:
    if all(par is None for par in (bronAsset, agent, rol)):
        raise ValueError("At least one of the parameters 'bronAsset', 'agent', or 'rol' must be provided.")

    query = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                     selection=SelectionDTO(expressions=[
                         ExpressionDTO(
            terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=actief)],
            logicalOp=None)]))
    if bronAsset:
        expression_bronAsset = ExpressionDTO(
            terms=[TermDTO(property='bronAsset', operator=OperatorEnum.EQ, value=f'{bronAsset.uuid}')],
            logicalOp=LogicalOpEnum.AND)
        query.selection.expressions.append(expression_bronAsset)
    if agent:
        expression_agent = ExpressionDTO(
            terms=[TermDTO(property='agent', operator=OperatorEnum.EQ, value=f'{agent.uuid}')], logicalOp=LogicalOpEnum.AND)
        query.selection.expressions.append(expression_agent)
    if rol:
        expression_rol = ExpressionDTO(terms=[TermDTO(property='rol', operator=OperatorEnum.EQ, value=rol)],
                                         logicalOp=LogicalOpEnum.AND)
        query.selection.expressions.append(expression_rol)
    return query
