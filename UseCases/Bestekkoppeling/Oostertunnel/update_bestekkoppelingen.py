import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

from UseCases.utils import load_settings_path
from utils.date_helpers import format_datetime
from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling
from API.Enums import AuthType, Environment

ENVIRONMENT = Environment.TEI
# BESTANDSNAAM = f'Import {ENVIRONMENT.name}_OOS klaar v Prod met bestek.xlsx'
BESTANDSNAAM = f'Import {ENVIRONMENT.name}_Pos2 klaar v Prod met bestek.xlsx'



def read_excel_as_df(filepath: Path, usecols: list = None) -> pd.DataFrame:
    if not Path.exists(filepath):
        raise FileExistsError(f'Filepath "{filepath}" does not exist.')

    if not usecols:
        usecols = ['id', 'naampad', '1e bestek', 'Startdatum 1e bestek', '2e bestek', 'Startdatum 2e bestek',
                   '3e bestek', 'Startdatum 3e bestek', 'type', 'actief', 'toestand']

    df = pd.read_excel(filepath, sheet_name='Sheet0', header=0, usecols=usecols)
    df = df.rename(columns={
        'id': 'uuid',
        '1e bestek': 'bestek1_naam', 'Startdatum 1e bestek': 'bestek1_datum', '2e bestek': 'bestek2_naam',
        'Startdatum 2e bestek': 'bestek2_datum', '3e bestek': 'bestek3_naam', 'Startdatum 3e bestek': 'bestek3_datum'})
    df = df.dropna(subset=["uuid"])
    return df


def get_bestek_info(bestek_naam: str) -> list[datetime | str]:
    """
    Returns startdatum and enddatum from a bestek

    :param bestek_naam: Naam van het bestek
    :type bestek_naam: str
    :return: tuple[str, str]

    """
    besteknaam_dict = {
        "INTERN-2129": {
            "startDatum": datetime(2018, 2, 22),
            "eindDatum": ""
        },
        "INTERN-2130": {
            "startDatum": datetime(2018, 2, 22),
            "eindDatum": ""
        },
        "INTERN-2131": {
            "startDatum": datetime(2018, 2, 22),
            "eindDatum": ""
        },
        "WA/INV/TNL/2024/2": {
            "startDatum": datetime(2024, 12, 24),
            "eindDatum": datetime(2028, 12, 23)
        },
        "WA/INV/TNL/2021/2": {
            "startDatum": datetime(2022, 7, 16),
            "eindDatum": datetime(2026, 12, 31)
        },
        "WA/OND/TNL/2023/1": {
            "startDatum": datetime(2024, 8, 5),
            "eindDatum": datetime(2028, 12, 23)
        },
        "WA/OND/TNL/2023/2": {
            "startDatum": datetime(2024, 12, 24),
            "eindDatum": datetime(2028, 12, 23)
        },
        "WA/OND/TNL/2021/1/P2": {
            "startDatum": datetime(2022, 7, 16),
            "eindDatum": datetime(2026, 12, 31)
        },
        "WA/OND/TNL/2021/1/P1": {
            "startDatum": datetime(2022, 8, 24),
            "eindDatum": ""
        }
    }
    bestek_info = besteknaam_dict[bestek_naam]
    return [bestek_info["startDatum"], bestek_info["eindDatum"]]


def process_assets_add_bestekkoppelingen(df: pd.DataFrame) -> None:
    """
    Toevoegen bestekkoppelingen voor een asset op basis van diens ID.
    Bestaande bestekkoppelingen blijven bewaard.
    Nieuwe bestekkoppelingen worden achteraan toegevoegd.
    Uiteindelijk worden alle bestekkoppelingen gesorteerd en de inactieve verschijnen achteraan.

    :param df:
    :return: None
    """
    for idx, df_row in df.iterrows():
        logging.debug(f'Processing asset ({int(idx) + 1}/{len(df)}):'
                      f'\nnaampad: {df_row["naampad"]}'
                      f'\nasset_uuid: {df_row["uuid"]}')
        update_bestekkoppelingen = False
        asset = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=df_row["uuid"])

        if not asset:
            log_message = 'Asset onbestaande. Maak eerst de asset aan.'
            logging.warning(log_message)
            raise ValueError(log_message)

        # ophalen van de huidige/bestaande/actuele bestekkoppeling
        bestekkoppelingen = eminfra_client.bestek_service.get_bestekkoppeling_by_uuid(asset_uuid=asset.uuid)

        # ophalen van de bestek naam uit het Excel-bestand
        for i in range(1, 4):
            bestek_naam = df_row[f"bestek{i}_naam"]
            logging.debug('Skip empty values of Bestekken')
            if pd.isna(bestek_naam):
                continue
            update_bestekkoppelingen = True
            bestek_startdatum, bestek_einddatum = get_bestek_info(bestek_naam=bestek_naam)
            logging.info(f'Appending bestek: {bestek_naam} with start_date: {bestek_startdatum} '
                         f'and end_date: {bestek_einddatum}')

            bestekref_new = eminfra_client.bestek_service.get_bestekref(eDelta_dossiernummer=bestek_naam)

            # Check if bestekkoppeling exists: edit startdatum and einddatum
            if matching_koppeling := next(
                    (k for k in bestekkoppelingen if k.bestekRef.uuid == bestekref_new.uuid), None, ):
                logging.debug(f'Bestekkoppeling "{bestekref_new.eDeltaBesteknummer}" bestaat reeds, '
                              f'start- en einddatum wordt geüpdatet.')
                matching_koppeling.startDatum = format_datetime(bestek_startdatum) if bestek_startdatum else None
                matching_koppeling.eindDatum = format_datetime(bestek_einddatum) if bestek_einddatum else None
            # Add new bestekkoppeling
            else:
                logging.debug(
                    f'Bestekkoppeling "{bestekref_new.eDeltaBesteknummer}" bestaat nog niet, en wordt aangemaakt')
                bestekkoppeling_new = BestekKoppeling(
                    bestekRef=bestekref_new,
                    status=BestekKoppelingStatusEnum.ACTIEF,
                    startDatum=format_datetime(bestek_startdatum) if bestek_startdatum else None,
                    eindDatum=format_datetime(bestek_einddatum) if bestek_einddatum else None,
                    categorie=BestekCategorieEnum.WERKBESTEK
                )
                # Insert the new bestekkoppeling at the end (not specifically in front of index position 0)
                bestekkoppelingen.append(bestekkoppeling_new)

        if update_bestekkoppelingen:
            # Herorden de volgorde van de bestekkoppelingen: alle inactieve onderaan de lijst.
            bestekkoppelingen = sorted(bestekkoppelingen, key=lambda x: x.status.value, reverse=False)

            # Update all the bestekkoppelingen for this asset
            eminfra_client.bestek_service.change_bestekkoppelingen_by_uuid(
                asset_uuid=asset.uuid, bestekkoppelingen=bestekkoppelingen)


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t',
                        filemode="w")
    logging.info('Bestekkoppelingen aanpassen voor assets van de Tunnels')

    logging.info(f'Omgeving: {ENVIRONMENT.name}')
    eminfra_client = EMInfraClient(auth_type=AuthType.JWT, env=ENVIRONMENT, settings_path=load_settings_path())

    # Read Excel as pandas dataframe
    excel_file = (Path.home() / 'OneDrive - Vlaamse overheid - Office 365' / '1_AWVGeneric' /
                      '192_importeren_tunnelbomen_oostertunnel_A2595' / 'input' / ENVIRONMENT.name / BESTANDSNAAM)

    df_assets = read_excel_as_df(filepath=excel_file)

    process_assets_add_bestekkoppelingen(df=df_assets)
