import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import BestekCategorieEnum, BestekKoppelingStatusEnum, BestekRef, BestekKoppeling
from API.Enums import AuthType, Environment
from UseCases.utils import configure_logger, load_settings
from utils.date_helpers import format_datetime

EINDDATUM = datetime(year=2026, month=12, day=10, hour=0, minute=0)
OUTPUT_EXCEL_PATH = Path('Verlengen_bestekkoppeling_MDM_19H01.xlsx')
EDELTA_BESTEKNUMMER = 'MDM/19H01'


def verleng_bestekkoppelingen(client: EMInfraClient,
                              df: pd.DataFrame,
                              bestekref: BestekRef,
                              einddatum: datetime = EINDDATUM,
                              output_excel_path: Path = OUTPUT_EXCEL_PATH) -> Path:
    """
    Verleng de einddatum van bestekkoppelingen voor een lijst van uuid's (assets) in een Dataframe

    :param client:
    :type client:
    :param df: Dataframe met kolom uuid
    :type df: pd.Dataframe
    :param bestekref: Bestek referentie
    :type bestekref: BestekRef
    :param einddatum: einddatum waarnaar de bestekkoppeling wordt verlengd.
    :type einddatum: datetime
    :param output_excel_path: output Excel path
    :type output_excel_path: Path
    :return output file path
    """
    assets_updated = []
    for counter, df_row in df.iterrows():
        asset = client.asset_service.get_asset_by_uuid(asset_uuid=df_row["uuid"])
        logging.debug(f'Processing ({counter}) asset: {asset.uuid}; naam: {asset.naam}; assettype: {asset.type.uri}')

        bestekkoppelingen: list[BestekKoppeling] = client.bestek_service.get_bestekkoppeling(asset=asset)

        index, matching_bestekkoppeling = next(((i, x) for i, x in enumerate(bestekkoppelingen)
                                                if x.status == BestekKoppelingStatusEnum.INACTIEF
                                                and x.bestekRef.uuid == bestekref.uuid
                                                and x.categorie == BestekCategorieEnum.WERKBESTEK), (None, None))

        if (matching_bestekkoppeling and
                datetime.fromisoformat(matching_bestekkoppeling.eindDatum).date() ==
                datetime(year=2026, month=1, day=8).date()):
            logging.debug(f'Verleng bestekkoppeling {matching_bestekkoppeling.bestekRef.eDeltaBesteknummer}')
            bestekkoppelingen[index].eindDatum = format_datetime(einddatum)
            # Update all the bestekkoppelingen for this asset
            assets_updated.append({
                "uuid": asset.uuid, "naam": asset.naam, "besteknummer": bestekref.eDeltaBesteknummer,
                "Start koppeling": matching_bestekkoppeling.startDatum,
                "Einde koppeling": matching_bestekkoppeling.eindDatum,
                "eminfra_link": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}'
            })
            client.bestek_service.change_bestekkoppelingen(asset=asset, bestekkoppelingen=bestekkoppelingen)
        else:
            logging.debug('geen overeenkomstige bestekkoppeling gevonden, ga verder met de volgende asset.')
            continue

    df = pd.DataFrame(assets_updated)
    mode: str = 'a' if Path(output_excel_path).exists() else 'w'
    if mode == 'a':
        with pd.ExcelWriter(output_excel_path, mode=mode, engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name='overzicht', index=False, freeze_panes=(1, 1))
    elif mode == 'w':
        with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='overzicht', index=False, freeze_panes=[1, 1])
    return output_excel_path


if __name__ == '__main__':
    configure_logger()
    logging.info('Verlengen van bestekkoppeling "MDM/19H01" naar 10/12/2026.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info("Query assets...")
    input_filepath = Path.home() / 'Downloads' / 'MDM19H01.xlsx'
    df_assets = pd.read_excel(input_filepath, sheet_name='query-result (7)', header=0, usecols=["uuid", "naampad"])
    bestekref = eminfra_client.bestek_service.get_bestekref(eDelta_besteknummer=EDELTA_BESTEKNUMMER)
    verleng_bestekkoppelingen(client=eminfra_client, df=df_assets, bestekref=bestekref)
