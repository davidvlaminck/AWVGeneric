import logging
from pathlib import Path

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import (RelatieEnum, OperatorEnum, PagingModeEnum, SelectionDTO, QueryDTO, ExpressionDTO,
                               TermDTO, LogicalOpEnum)
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger

ASSETTYPE_KAST = '10377658-776f-4c21-a294-6c740b9f655e'


def build_search_query(client: EMInfraClient, assettype_uuid: str, relatie_richting: str,
                       relatie: RelatieEnum) -> QueryDTO:
    if relatie_richting not in ('in', 'out'):
        raise ValueError('Parameter relatie_richting moet een van volgende waardes zijn: "in", "out".')
    if relatie not in (RelatieEnum.STURING, RelatieEnum.BEVESTIGING):
        raise ValueError('Parameter relatie moet een niet-gerichte relatie zijn. Ofwel Bevestiging ofwel Sturing.')
    _, relatietype_id = client.get_kenmerktype_and_relatietype_id(relatie= relatie)
    query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[
                                     TermDTO(property='actief',
                                             operator=OperatorEnum.EQ,
                                             value=True),
                                     TermDTO(property=f'rel:{relatie_richting}:{relatietype_id}',
                                                operator=OperatorEnum.EQ,
                                                value=None,
                                                logicalOp=LogicalOpEnum.AND),
                                     TermDTO(property='type',
                                                operator=OperatorEnum.EQ,
                                                value=assettype_uuid,
                                                logicalOp=LogicalOpEnum.AND)
                                        ])]))
    logging.debug(f'Inspect query_dto:\n{query_dto}')
    return query_dto

if __name__ == '__main__':
    configure_logger()
    logging.info('Omkeren van niet-gerichte assetsrelaties.')
    eminfra_client = EMInfraClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=load_settings())

    search_query = build_search_query(client=eminfra_client, relatie_richting='in', relatie=RelatieEnum.BEVESTIGING,
                                      assettype_uuid=ASSETTYPE_KAST)
    generator = eminfra_client.search_assets(query_dto=search_query)

    rows = []
    for idx, asset in enumerate(generator):
        logging.info(f"Processing asset: ({idx}): asset_uuid: {asset.uuid}")

        logging.info('Oplijsten van doel-assets')
        assets_gelinkt = eminfra_client.search_assets_via_relatie(asset_uuid=asset, relatie=RelatieEnum.BEVESTIGING)
        logging.debug('Filter doelassets')
        assets_ls = [asset for asset in assets_gelinkt if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        assets_lsdeel = [asset for asset in assets_gelinkt if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']

        for asset_ls in assets_ls:
            logging.info('Verwijder de inkomende relatie.')
            eminfra_client.remove_assets_via_relatie(bronasset_uuid=asset.uuid, doelasset_uuid=asset_ls, relatie=RelatieEnum.BEVESTIGING)

            logging.info('Voeg een uitgaande relatie toe.')
            eminfra_client.create_assetrelatie(bronAsset=asset, doelAsset=asset_ls, relatie=RelatieEnum.BEVESTIGING)


        #
        # bronasset_uuid = df_row["bronuuid"]
        # doelasset_uuid = df_row["doeluuid"]
        # relatie = df_row["relatie"]
        # if relatie == 'Bevestiging':
        #     rel = RelatieEnum.BEVESTIGING
        # elif relatie == 'Sturing':
        #     rel = RelatieEnum.STURING
        # else:
        #     raise ValueError(f'Relatie {relatie} not found.')
        # eminfra_client.remove_assets_via_relatie(bronasset_uuid=bronasset_uuid, doelasset_uuid=doelasset_uuid, relatie=rel)
        # row = {
        #     "bronasset_uuid": bronasset_uuid,
        #     "doelasset_uuid": doelasset_uuid
        # }
        # rows.append(row)

    # output_excel_path = 'assets_relaties_verwijderd.xlsx'
    # with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
    #     df = pd.DataFrame(rows)
    #     df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])