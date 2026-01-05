import logging

from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import QueryDTO, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, PagingModeEnum, \
    ExpansionsDTO, LocatieKenmerk
from API.Enums import AuthType, Environment
import pandas as pd

from Generic.ExcelModifier import ExcelModifier
from UseCases.utils import load_settings

print(""""
        Opvissen van alle assets met een bepaalde toezichter, diens locatie en de provincie waarbinnen ze zijn gelegen.
      """)

def build_lgc_query(toezichter_uuid: str = '07f9e78f-e341-4324-8c57-d9d2b46932f1') -> QueryDTO:
    """"""
    return QueryDTO(
        size=100
        , from_=0
        , expansions=ExpansionsDTO(fields=['parent'])
        , pagingMode=PagingModeEnum.OFFSET
        , selection=SelectionDTO(
            expressions=[ExpressionDTO(terms=[TermDTO(property='toezichter', operator=OperatorEnum.EQ, value=toezichter_uuid)])]))

def build_otl_query(agent_uuid: str = '8a7c6f90-23b8-4170-b6b7-1517b8c8465b:toezichter') -> QueryDTO:
    """"""
    return QueryDTO(
        size=100
        , from_=0
        , pagingMode=PagingModeEnum.OFFSET
        , selection=SelectionDTO(
            expressions=[ExpressionDTO(terms=[TermDTO(property='agent', operator=OperatorEnum.EQ, value=agent_uuid)])]))


def parse_provincie_gemeente(locatiekenmerk: LocatieKenmerk):
    provincie = gemeente = None
    if locatie := locatiekenmerk.locatie:
        adres = locatie.get("adres", {})
        provincie = adres.get("provincie")
        gemeente = adres.get("gemeente")
    return provincie, gemeente



if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Use case naam:\t use case beschrijving')
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)
    file_path = 'installaties_toezichter_DaveGeudens.xlsx'

    lgc_assets_generator = eminfra_client.search_all_assets(query_dto=build_lgc_query())
    otl_assets_generator = eminfra_client.search_all_assets(query_dto=build_otl_query())

    print('Downloading...')

    columns = ["provincie", "gemeente", "beheerobject", "naampad", "naam", "uri", "uuid", "toestand", "actief", "toezichter", "geometry", "LGC/OTL"]
    data_list = []

    ## Legacy assets
    for asset in lgc_assets_generator:
        locatie_kenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)
        provincie, gemeente = parse_provincie_gemeente(locatiekenmerk=locatie_kenmerk)

        beheerobject = eminfra_client.search_parent_asset(asset_uuid=asset.uuid, return_all_parents=False, recursive=True)

        # Prepare row data (this can be a list or a dictionary)
        row_data = {
            "provincie": provincie
            , "gemeente": gemeente
            , "beheerobject": f'{beheerobject.naam}'
            , "naampad": None
            , "naam": f'{asset.naam}'
            , "uri": f'{asset.type.uri}'
            , "uuid": f'{asset.uuid}'
            , "toestand": f'{asset.toestand.name}'
            , "actief": f'{asset.actief}'
            , "toezichter": 'Dave Geudens'
            , "geometry": f'{locatie_kenmerk.geometrie}'
            , "LGC/OTL": 'Legacy'
        }

        # Append the row data to the list
        data_list.append(row_data)

    ## OTL assets
    for asset in otl_assets_generator:
        locatie_kenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)
        provincie, gemeente = parse_provincie_gemeente(locatiekenmerk=locatie_kenmerk)

        # Prepare row data (this can be a list or a dictionary)
        row_data = {
            "provincie": provincie
            , "gemeente": gemeente
            , "beheerobject": None
            , "naampad": None
            , "naam": f'{asset.naam}'
            , "uri": f'{asset.type.uri}'
            , "uuid": f'{asset.uuid}'
            , "toestand": f'{asset.toestand.name}'
            , "actief": f'{asset.actief}'
            , "toezichter": 'Dave Geudens'
            , "geometry": f'{locatie_kenmerk.geometrie}'
            , "LGC/OTL": 'OTL'
        }

        # Append the row data to the list
        data_list.append(row_data)

    # Write to Excel
    df = pd.DataFrame(data_list, columns=columns)
    df.to_excel(file_path, index=False, freeze_panes=[1,2])

    # Insert an em-infra link
    ExcelModifier(file_path=file_path).add_hyperlink()