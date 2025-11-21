import logging
from datetime import datetime

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, BestekCategorieEnum, BestekKoppelingStatusEnum
from API.Enums import AuthType, Environment
from UseCases.utils import configure_logger, load_settings
from utils.date_helpers import format_datetime

ASSETTYPE_UUID_NETWERKELEMENT = 'b6f86b8d-543d-4525-8458-36b498333416'
EINDDATUM = datetime(year=2025, month=11, day=21, hour=12)
OUTPUT_EXCEL_PATH = 'Netwerkelementen_Swarco_VWT-NET-2020-017.xlsx'

# todo: reuse this function from the utils.
def build_query_search_assettype(assettype_uuid: str) -> QueryDTO:
    return QueryDTO(
        size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(
                        property='type',
                        operator=OperatorEnum.EQ,
                        value=f"{assettype_uuid}")]
                ),
                ExpressionDTO(
                    terms=[TermDTO(
                        property='actief',
                        operator=OperatorEnum.EQ,
                        value=True)]
                    , logicalOp=LogicalOpEnum.AND)
            ]))


if __name__ == '__main__':
    configure_logger()
    logging.info('Beëindigen van bestekkoppeling "VWT/NET/2020/017" bij alle Netwerkelementen.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Get all assets of assettype Netwerkelement.")
    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_NETWERKELEMENT)
    generator = eminfra_client.search_assets(query_dto=search_query, actief=True)

    assets_updated = []
    for counter, asset in enumerate(generator, start=1):
        logging.debug(f'Processing ({counter}) asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)
        werkbestekken = [k for k in bestekkoppelingen if k.categorie == BestekCategorieEnum.WERKBESTEK and k.status == BestekKoppelingStatusEnum.ACTIEF] # test dit.

        bestekRef_swarco_2020_17 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer='VWT/NET/2020/017')
        index, matching_bestekkoppeling = next(((i, x) for i, x in enumerate(bestekkoppelingen) if x.bestekRef.uuid == bestekRef_swarco_2020_17.uuid), (None, None))

        logging.debug('Is de bestekkoppeling van Swarco een van de bestaande bestekkoppelingen?'
                      '\nEN is er minstens 1 ander actief werkbestek?')
        if index and matching_bestekkoppeling and len(werkbestekken) >= 2:
            bestekkoppelingen[index].eindDatum = format_datetime(EINDDATUM)
            # Update all the bestekkoppelingen for this asset
            assets_updated.append({
                "uuid": asset.uuid
                , "eminfra_link": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}'
            })
            eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)
        else:
            logging.debug('Ga verder met het volgende Netwerkelement,'
                          'geen overeenkomstige bestekkoppeling of minstens 2 werkbestekken.')
            continue

    with pd.ExcelWriter(OUTPUT_EXCEL_PATH, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(assets_updated)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])