import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import QueryDTO, BestekCategorieEnum, BestekKoppelingStatusEnum, BestekRef
from API.Enums import AuthType, Environment
from UseCases.utils import configure_logger, load_settings
from utils.date_helpers import format_datetime
from utils.query_dto_helpers import build_query_search_assettype

ASSETTYPE_UUID_BUS = '18b0702e-dd5c-439b-98b5-dafb23bee29b'  # Beïnvloeding openbaar vervoer
ASSETTYPE_UUID_KAR = 'f3b7255f-c385-4c0d-94d3-50fbe9c35975'  # Korte afstandsradio
EINDDATUM = datetime(year=2026, month=6, day=2, hour=23, minute=59)
OUTPUT_EXCEL_PATH = 'Verlengen_bestekkoppeling_SPIE.xlsx'
EDELTA_DOSSIERNUMMER = 'VWT/VL/2021/3'

def verleng_bestekkoppelingen(eminfra_client: EMInfraClient,
                              search_query: QueryDTO,
                              bestekRef: BestekRef,
                              assettype: str,
                              einddatum: datetime = EINDDATUM,
                              dossiernummer: str = EDELTA_DOSSIERNUMMER,
                              output_excel_path: Path() = OUTPUT_EXCEL_PATH) -> None:
    generator = eminfra_client.search_assets(query_dto=search_query, actief=True)
    assets_updated = []
    for counter, asset in enumerate(generator, start=1):
        logging.debug(f'Processing ({counter}) asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

        index, matching_bestekkoppeling = next(((i, x) for i, x in enumerate(bestekkoppelingen)
                                                if x.status == BestekKoppelingStatusEnum.INACTIEF
                                                and x.bestekRef.uuid == bestekRef.uuid
                                                and x.categorie == BestekCategorieEnum.WERKBESTEK), (None, None))

        if matching_bestekkoppeling and datetime.fromisoformat(matching_bestekkoppeling.eindDatum).date() == datetime(year=2025, month=12, day=3).date():
            logging.debug(f'Verleng bestekkoppeling {matching_bestekkoppeling.bestekRef.eDeltaBesteknummer}')
            bestekkoppelingen[index].eindDatum = format_datetime(einddatum)
            # Update all the bestekkoppelingen for this asset
            assets_updated.append({
                "uuid": asset.uuid
                , "naam": asset.naam
                , "dossiernummer": dossiernummer
                , "Start koppeling": matching_bestekkoppeling.startDatum
                , "Einde koppeling": matching_bestekkoppeling.eindDatum
                , "eminfra_link": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}'
            })
            eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)
        else:
            logging.debug('geen overeenkomstige bestekkoppeling gevonden, ga verder met de volgende asset.')
            continue

    df = pd.DataFrame(assets_updated)
    mode = 'a' if Path(output_excel_path).exists() else 'w'
    if mode == 'a':
        with pd.ExcelWriter(output_excel_path, mode=mode, engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=f'overzicht_{assettype}', index=False, freeze_panes=(1, 1))
    elif mode == 'w':
        with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=f'overzicht_{assettype}', index=False, freeze_panes=[1, 1])

if __name__ == '__main__':
    configure_logger()
    logging.info('Verlengen van bestekkoppeling "VWT/VL/2021/3" bij alle BUS en KAR van 3/12/2025 naar 02/06/2025.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Query assets...")
    search_query_BUS = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_BUS)
    search_query_KAR = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_KAR)
    bestekRef_SPIE = eminfra_client.get_bestekref_by_eDelta_dossiernummer(
        eDelta_dossiernummer=EDELTA_DOSSIERNUMMER)

    verleng_bestekkoppelingen(eminfra_client=eminfra_client, search_query=search_query_BUS, bestekRef=bestekRef_SPIE, assettype='BUS')
    verleng_bestekkoppelingen(eminfra_client=eminfra_client, search_query=search_query_KAR, bestekRef=bestekRef_SPIE, assettype='KAR')