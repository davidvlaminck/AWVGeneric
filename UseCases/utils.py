from typing import Any

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


# new helper at top‐level
def _append_eq_expr(expressions: list[ExpressionDTO],
                    prop: str,
                    value: Any) -> None:
    expressions.append(
        ExpressionDTO(
            terms=[TermDTO(property=prop,
                           operator=OperatorEnum.EQ,
                           value=value)],
            logicalOp=LogicalOpEnum.AND if expressions else None
        )
    )
def build_query_search_betrokkenerelaties(bronAsset: AssetDTO = None, agent: AgentDTO = None, rol: str = None) -> QueryDTO:
    if all(par is None for par in (bronAsset, agent, rol)):
        raise ValueError("At least one of the parameters 'bronAsset', 'agent', or 'rol' must be provided.")
    exprs: list[ExpressionDTO] = []
    if bronAsset:
        _append_eq_expr(exprs, "bronAsset", bronAsset.uuid)
    if agent:
        _append_eq_expr(exprs, "agent", agent.uuid)
    if rol:
        _append_eq_expr(exprs, "rol", rol)
    return QueryDTO(
        size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(expressions=exprs)
    )


def build_query_search_assets(bronAsset: AssetDTO = None, agent: AgentDTO = None,
                                          actief: bool = True) -> QueryDTO:
    if all(par is None for par in (bronAsset, agent)):
        raise ValueError("At least one of the parameters 'bronAsset', 'agent' must be provided.")
    exprs: list[ExpressionDTO] = []
    # always start with actief
    _append_eq_expr(exprs, "actief", actief)
    if bronAsset:
        _append_eq_expr(exprs, "bronAsset", bronAsset.uuid)
    if agent:
        _append_eq_expr(exprs, "agent",    agent.uuid)
    return QueryDTO(
        size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(expressions=exprs)
    )
