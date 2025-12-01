import logging
from typing import Any

import pandas as pd
from pathlib import Path

from API.EMInfraDomain import AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    AgentDTO, LogicalOpEnum, ExpansionsDTO
from collections.abc import Generator
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO
from otlmow_model.OtlmowModel.Classes.ImplementatieElement.RelatieObject import RelatieObject
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation

def load_settings(user: str = 'Dries'):
    if user == 'Dries':
        return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    else:
        raise NotImplementedError(f'user: {user} is not implemented in function call load_settings()')

def configure_logger(log_path: str = "logs.log"):
    logging.basicConfig(
        filename=log_path, level=logging.DEBUG,
        format="%(levelname)s:\t%(asctime)s:\t%(message)s",
        filemode="w",
    )

def read_rsa_report(filepath: Path, usecols: [str] = None) -> pd.DataFrame:
    """Read RSA-report as input into a DataFrame."""
    if not Path.exists(filepath):
        raise FileNotFoundError(f'Filepath does not exists {filepath}.')
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

def build_query_search_by_naampad(naampad: str) -> QueryDTO:
    query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET, selection=SelectionDTO(expressions=[])
                         , expansions=ExpansionsDTO(fields=['parent']))
    expression = ExpressionDTO(terms=[
        TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH, value=f'{naampad}')])
    query_dto.selection.expressions.append(expression)
    return query_dto


def get_bestaande_betrokkenerelaties(client: EMInfraClient, asset: AssetDTO, rol: str, isActief: bool) -> Generator[RelatieObject]:
    generator = client.get_objects_from_oslo_search_endpoint(
        url_part='betrokkenerelaties'
        , filter_dict={"bronAsset": asset.uuid, 'rol': rol})

    for item in generator:
        betrokkenerelatie_uuid = item['RelatieObject.assetId']['DtcIdentificator.identificator']
        relatie = create_betrokkenerelation(
            rol=rol,
            source_typeURI=item['RelatieObject.bron']['@type'],
            source_uuid=item['RelatieObject.bronAssetId']['DtcIdentificator.identificator'][:36],
            target_typeURI=item['RelatieObject.doel']['@type'],
            target_uuid=item['RelatieObject.doelAssetId']['DtcIdentificator.identificator'][:36],
        )
        relatie.assetId.identificator = betrokkenerelatie_uuid  # Assign existing UUID
        relatie.isActief = isActief
        yield relatie


def build_betrokkenerelatie(client: EMInfraClient, source: AssetDTO, agent_naam :str, rol: str) -> RelatieObject | None:
    generator_agents = client.get_objects_from_oslo_search_endpoint(
        url_part='agents'
        , filter_dict={"naam": agent_naam})
    agents = list(generator_agents)
    if len(agents) != 1:
        return None
    agent_uri = agents[0].get('@type')
    agent_uuid = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]

    return create_betrokkenerelation(rol=rol
                                     , source_typeURI=source.type.uri
                                     , source_uuid=source.uuid
                                     , target_uuid=agent_uuid
                                     , target_typeURI=agent_uri)
