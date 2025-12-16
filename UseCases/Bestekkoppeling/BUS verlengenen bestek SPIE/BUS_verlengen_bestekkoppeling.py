import logging
from datetime import datetime

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, BestekCategorieEnum, BestekKoppelingStatusEnum
from API.Enums import AuthType, Environment
from UseCases.utils import configure_logger, load_settings
from utils.date_helpers import format_datetime
from utils.query_dto_helpers import build_query_search_assettype, add_expression

ASSETTYPE_UUID_BUS = '18b0702e-dd5c-439b-98b5-dafb23bee29b'
EINDDATUM = datetime(year=2026, month=6, day=2, hour=23, minute=59)
OUTPUT_EXCEL_PATH = 'BUS_verlengen_bestekkoppeling.xlsx'
EDELTA_DOSSIERNUMMER = 'VWT/VL/2021/3'

if __name__ == '__main__':
    configure_logger()
    logging.info('Verlengen van bestekkoppeling "VWT/VL/2021/3" bij alle BUS van 3/12/2025 naar 02/06/2025.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Query assets...")
    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_BUS)
    bestekRef_SPIE = eminfra_client.get_bestekref_by_eDelta_dossiernummer(
        eDelta_dossiernummer=EDELTA_DOSSIERNUMMER)
    expression_bestek = ExpressionDTO(logicalOp=LogicalOpEnum.AND,
                               terms=[TermDTO(property='bestek', operator=OperatorEnum.EQ, value=bestekRef_SPIE.uuid)])
    search_query.selection.expressions.append(expression_bestek)

    generator = eminfra_client.search_assets(query_dto=search_query, actief=True)

    assets_updated = []
    for counter, asset in enumerate(generator, start=1):
        logging.debug(f'Processing ({counter}) asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)
        # actieve_bestekken = [k for k in bestekkoppelingen if k.status == BestekKoppelingStatusEnum.ACTIEF]
        # actieve_bestekken_set = {
        #     b.bestekRef.eDeltaDossiernummer for b in actieve_bestekken
        # }
        # nbr_actieve_bestekken = len(actieve_bestekken_set)

        index, matching_bestekkoppeling = next(((i, x) for i, x in enumerate(bestekkoppelingen)
                                                if x.status == BestekKoppelingStatusEnum.INACTIEF
                                                and x.bestekRef.uuid == bestekRef_SPIE.uuid
                                                and x.categorie == BestekCategorieEnum.WERKBESTEK), (None, None))

        if matching_bestekkoppeling and datetime.fromisoformat(matching_bestekkoppeling.startDatum).date() == datetime(year=2025, month=12, day=3).date():
            logging.debug(f'Verleng bestekkoppeling {matching_bestekkoppeling.bestekRef.eDeltaBesteknummer}')
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
            # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)
        else:
            logging.debug('Ga verder met de volgende BUS asset,'
                          'geen overeenkomstige bestekkoppeling gevonden.')
            continue

    with pd.ExcelWriter(OUTPUT_EXCEL_PATH, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(assets_updated)
        df.to_excel(writer, sheet_name='overzicht', index=False, freeze_panes=[1, 1])