import logging
from datetime import datetime

import pandas as pd

from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import BestekCategorieEnum, BestekKoppelingStatusEnum
from API.Enums import AuthType, Environment
from UseCases.utils import configure_logger, load_settings
from utils.query_dto_helpers import build_query_search_assettype

ASSETTYPE_UUID_NETWERKELEMENT = 'b6f86b8d-543d-4525-8458-36b498333416'
SPECIFIC_DATE = datetime(year=2025, month=11, day=21).date()
OUTPUT_EXCEL_PATH = 'Netwerkelementen_Swarco_VWT-NET-2020-017_verwijderen.xlsx'


if __name__ == '__main__':
    configure_logger()
    logging.info('Verwijderen van inactieve bestekkoppeling "VWT/NET/2020/017".')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Get all assets of assettype Netwerkelement.")
    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_NETWERKELEMENT)
    generator = eminfra_client.search_assets(query_dto=search_query, actief=True)

    bestekRef_swarco_2020_17 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer='VWT/NET/2020/017')
    assets_updated = []
    for counter, asset in enumerate(generator, start=1):
        logging.debug(f'Processing ({counter}) asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)
        index = None
        matching_bestekkoppeling = None

        for i, koppeling in enumerate(bestekkoppelingen):
            if (
                    koppeling.status == BestekKoppelingStatusEnum.INACTIEF
                    and koppeling.bestekRef.uuid == bestekRef_swarco_2020_17.uuid
                    and koppeling.categorie == BestekCategorieEnum.WERKBESTEK
            ):
                index = i
                matching_bestekkoppeling = koppeling
                break

        if matching_bestekkoppeling:
            start_date = datetime.fromisoformat(matching_bestekkoppeling.startDatum).date()
            if start_date == SPECIFIC_DATE:
                logging.debug(f'Verwijder bestekkoppeling {matching_bestekkoppeling.bestekRef.eDeltaBesteknummer}')
                assets_updated.append({
                    "uuid": asset.uuid,
                    "naam": asset.naam,
                    "bestek": 'VWT/NET/2020/017',
                    "Start koppeling": matching_bestekkoppeling.startDatum,
                    "Einde koppeling": matching_bestekkoppeling.eindDatum,
                    "eminfra_link": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}',
                })
                del bestekkoppelingen[index]
                eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)

    with pd.ExcelWriter(OUTPUT_EXCEL_PATH, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(assets_updated)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])