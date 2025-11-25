import logging
from datetime import datetime

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, BestekCategorieEnum, BestekKoppelingStatusEnum
from API.Enums import AuthType, Environment
from UseCases.utils import configure_logger, load_settings
from utils.date_helpers import format_datetime
from utils.query_dto_helpers import build_query_search_assettype

ASSETTYPE_UUID_NETWERKELEMENT = 'b6f86b8d-543d-4525-8458-36b498333416'
EINDDATUM = datetime(year=2025, month=11, day=21, hour=14)
OUTPUT_EXCEL_PATH = 'Netwerkelementen_Swarco_VWT-NET-2020-017.xlsx'


if __name__ == '__main__':
    configure_logger()
    logging.info('Beëindigen van bestekkoppeling "VWT/NET/2020/017" bij alle Netwerkelementen op voorwaarde dat er minstens 1 ander actief bestek aan de asset gekoppeld is.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Get all assets of assettype Netwerkelement.")
    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_NETWERKELEMENT)
    generator = eminfra_client.search_assets(query_dto=search_query, actief=True)

    assets_updated = []
    for counter, asset in enumerate(generator, start=1):
        logging.debug(f'Processing ({counter}) asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)
        actieve_bestekken = [k for k in bestekkoppelingen if k.status == BestekKoppelingStatusEnum.ACTIEF]
        actieve_bestekken_set = {
            b.bestekRef.eDeltaDossiernummer for b in actieve_bestekken
        }
        nbr_actieve_bestekken = len(actieve_bestekken_set)

        bestekRef_swarco_2020_17 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer='VWT/NET/2020/017')
        index, matching_bestekkoppeling = next(((i, x) for i, x in enumerate(bestekkoppelingen) if x.status == BestekKoppelingStatusEnum.ACTIEF and x.bestekRef.uuid == bestekRef_swarco_2020_17.uuid and x.categorie == BestekCategorieEnum.WERKBESTEK), (None, None))

        logging.debug('Is de bestekkoppeling van Swarco een van de bestaande bestekkoppelingen?'
                      '\nEN is er minstens 1 ander actief bestek?')

        if matching_bestekkoppeling and nbr_actieve_bestekken >= 2 and datetime.fromisoformat(matching_bestekkoppeling.startDatum).date() == datetime(year=2025, month=11, day=21).date():
            logging.debug(f'Beëindig bestekkoppeling {matching_bestekkoppeling.bestekRef.eDeltaBesteknummer}')
            bestekkoppelingen[index].eindDatum = format_datetime(EINDDATUM)
            # Update all the bestekkoppelingen for this asset
            assets_updated.append({
                "uuid": asset.uuid
                , "naam": asset.naam
                , "bestek": 'VWT/NET/2020/017'
                , "Start koppeling": matching_bestekkoppeling.startDatum
                , "Einde koppeling": matching_bestekkoppeling.eindDatum
                , "eminfra_link": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}'
            })
            eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)
        else:
            logging.debug('Ga verder met het volgende Netwerkelement,'
                          'geen overeenkomstige bestekkoppeling of minstens 2 actieve werkbestekken.')
            continue

    with pd.ExcelWriter(OUTPUT_EXCEL_PATH, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(assets_updated)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])