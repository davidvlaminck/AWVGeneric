import logging

import pandas as pd
from pathlib import Path

from UseCases.utils import load_settings
from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, OperatorEnum, TermDTO, ExpressionDTO, \
    construct_naampad, ExpansionsDTO
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Ophalen uuid''s (PRODUCTIE) op basis van het naampad uit de (AIM-omgeving) voor de Tijsmanstunnel')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=load_settings())

    naampad_prefixen = ['TYS.TUNNEL', 'A2584']
    for naampad in naampad_prefixen:
        filepath_output = Path.home() / 'Downloads' / 'Bestekken_tunnels' / f'{naampad}_uuid_lookup.xlsx'

        query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET, selection=SelectionDTO(expressions=[])
                             , expansions=ExpansionsDTO(fields=['parent']))
        expression = ExpressionDTO(terms=[
            TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH, value=f'{naampad}')])
        query_dto.selection.expressions.append(expression)

        logging.info("Search assets")
        df_assets = eminfra_client.assets.search_assets(query_dto=query_dto, actief=True)
        rows = []
        for asset in iter(df_assets):
            row = {
                "uuid": asset.uuid
                , "naampad": construct_naampad(asset=asset)
            }
            rows.append(row)

        logging.info(f"Write to output file: {filepath_output}")
        with pd.ExcelWriter(filepath_output, mode='w', engine='openpyxl') as writer:
            df_output = pd.DataFrame(data=rows)
            df_output.to_excel(writer,  freeze_panes=[1,1], sheet_name='lookup', index=False)