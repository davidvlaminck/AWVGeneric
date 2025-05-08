import json
import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, QueryDTO, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, PagingModeEnum, \
    ExpansionsDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

print(""""
        Opvissen van alle assets met een bepaalde toezichter, diens locatie en de provincie waarbinnen ze zijn gelegen.
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def build_lgc_query(toezichter_uuid: str = '07f9e78f-e341-4324-8c57-d9d2b46932f1') -> None:
    """"""
    return QueryDTO(
        size=100
        , from_=0
        , expansions=ExpansionsDTO(fields=['parent'])
        , pagingMode=PagingModeEnum.OFFSET
        , selection=SelectionDTO(
            expressions=[ExpressionDTO(terms=[TermDTO(property='toezichter', operator=OperatorEnum.EQ, value=toezichter_uuid)])]))

def build_otl_query(agent_uuid: str = '07f9e78f-e341-4324-8c57-d9d2b46932f1') -> None:
    """"""
    query_dto = QueryDTO(
        size=100
        , from_=0
        , pagingMode=PagingModeEnum.OFFSET
        , selection=SelectionDTO(
            expressions=[ExpressionDTO(terms=[TermDTO(property='agent', operator=OperatorEnum.EQ, value=agent_uuid)])]))
    return query_dto

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Use case naam:\t use case beschrijving')
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    lgc_assets_generator = eminfra_client.search_all_assets(query_dto=build_lgc_query())
    otl_assets_generator = eminfra_client.search_all_assets(query_dto=build_otl_query())

    print('Downloading...')
    # Fill the rows and convert only once into a dataframe.
    all_lgc_assets = list(lgc_assets_generator)
    all_otl_assets = list(otl_assets_generator)

    columns = ["provincie", "gemeente", "beheerobject", "naampad", "naam", "uri", "uuid", "toestand", "actief", "toezichter", "geometry", "LGC/OTL"]
    data_list = []
    for asset in all_lgc_assets:
        # locatie_kenmerk =  eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)

        # Prepare row data (this can be a list or a dictionary)
        row_data = {
            "provincie": ''
            , "gemeente": ''
            , "beheerobject": ''
            , "naampad": ''
            , "naam": ''
            , "uri": ''
            , "uuid": ''
            , "toestand": ''
            , "actief": ''
            , "toezichter": 'Dave Geudens'
            , "geometry": ''
            , "LGC/OTL": 'Legacy'
        }

        # Append the row data to the list
        data_list.append(row_data)
    # idem OTL

    # Write to Excel

    # Insert an em-infra link