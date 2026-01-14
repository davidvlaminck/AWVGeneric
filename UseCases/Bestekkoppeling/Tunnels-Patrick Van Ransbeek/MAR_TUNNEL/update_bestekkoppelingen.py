import logging
from datetime import datetime

import pandas as pd
from pathlib import Path

from UseCases.utils import load_settings
from utils.date_helpers import format_datetime
from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling
from API.Enums import AuthType, Environment

BESTANDSNAAM = 'export_MAR v aanpassing bestekken 20260112_2.xlsx'


def read_excel_as_df(filepath: Path, usecols: list = None) -> pd.DataFrame:
    if not Path.exists(filepath):
        raise FileExistsError(f'Filepath "{filepath}" does not exist.')

    if not usecols:
        usecols = ['id', 'naampad', '1e bestek', '2e bestek', '3e bestek']

    df = pd.read_excel(filepath, sheet_name='Sheet0', header=0, usecols=usecols)
    df = df.rename(columns={
        'id': 'uuid',
        '1e bestek': 'bestek1_naam', 'Startdatum 1e bestek': 'bestek1_datum', '2e bestek': 'bestek2_naam',
        'Startdatum 2e bestek': 'bestek2_datum', '3e bestek': 'bestek3_naam', 'Startdatum 3e bestek': 'bestek3_datum'})
    df = df.dropna(subset=["uuid"])
    return df


def get_bestek_info(naam: str) -> dict:
    """
    Reiniging WA/INV/TNL/2024/2 Startbestek 24/12/2024 00:01 Eindbestek 31/12/2028 23:59
    NSA WA/OND/TNL/2023/2 Startbestek 24/12/2024 00:01 Eindbestek 23/12/2028 23:59
    Mechanisch WA/OND/TNL/2021/1 Startbestek 16/07/2022 00:01 Eindbestek 31/12/2026 23:59
    Elektrisch WA/OND/TNL/2023/1 Startbestek 05/08/2024 00:01 Eindbestek 24/12/2028 23:59
    """
    bestek_dict = {
        "INTERN-2129": {
            "bestekRef": "",
            "startDatum": "",
            "eindDatum": ""
        },
        "WA/INV/TNL/2024/2": {
            "bestekRef": "",
            "startDatum": "24/12/2024",
            "eindDatum": "31/12/2028"
        },
        "WA/OND/TNL/2023/2": {
            "bestekRef": "",
            "startDatum": "24/12/2024",
            "eindDatum": "23/12/2028"
        },
        "WA/OND/TNL/2021/1": {
            "bestekRef": "",
            "startDatum": "16/07/2022",
            "eindDatum": "31/12/2026"
        },
        "WA/OND/TNL/2023/1": {
            "bestekRef": "",
            "startDatum": "05/08/2024",
            "eindDatum": "24/12/2028"
        }
    }
    return bestek_dict[naam]
def process_assets(df: pd.DataFrame) -> None:
    """
    Toevoegen bestekkoppelingen voor een asset op basis van diens ID.
    Bestaande bestekkoppelingen blijven bewaard.
    Nieuwe bestekkoppelingen worden achteraan toegevoegd.
    Uiteindelijk worden alle bestekkoppelingen gesorteerd en de inactieve verschijnen achteraan.

    :param df:
    :return: None
    """
    for idx, df_row in df.iterrows():
        update_bestekkoppelingen = False
        logging.debug(f'Processing asset ({idx + 1}/{len(df)}):\nnaampad: {df_row["naampad"]}')
        logging.debug(f'Search by asset_id: {df_row["uuid"]}')
        asset = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=df_row["uuid"])

        if not asset:
            continue

        # search all bestekkoppelingen
        bestekkoppelingen = eminfra_client.bestek_service.get_bestekkoppeling_by_uuid(asset_uuid=asset.uuid)

        # ophalen van de huidige/bestaande/actuele bestekkoppeling op basis van de naam.
        for i in range(1, 4):
            bestek_naam = df_row[f"bestek{i}_naam"]
            logging.debug('Skip empty values of Bestekken')
            if pd.isna(bestek_naam):
                continue
            bestek_datum = datetime.strptime(df_row[f"bestek{i}_datum"], "%d/%m/%Y %H:%M")
            logging.info(f'Appending bestek: {bestek_naam} with start_date: {bestek_datum}')

            bestekRef_new = eminfra_client.bestek_service.get_bestekref(eDelta_dossiernummer=bestek_naam)

            # Check if the new bestekkoppeling doesn't exist and append at the end of the list, else do nothing
            if not (matching_koppeling := next(
                    (k for k in bestekkoppelingen if k.bestekRef.uuid == bestekRef_new.uuid),
                    None, )):
                logging.debug(
                    f'Bestekkoppeling "{bestekRef_new.eDeltaBesteknummer}" bestaat nog niet, en wordt aangemaakt')
                update_bestekkoppelingen = True
                bestekkoppeling_new = BestekKoppeling(
                    bestekRef=bestekRef_new,
                    status=BestekKoppelingStatusEnum.ACTIEF,
                    startDatum=format_datetime(bestek_datum),
                    eindDatum=None,
                    categorie=BestekCategorieEnum.WERKBESTEK
                )
                # Insert the new bestekkoppeling at the end (not specifically in front at index position 0)
                bestekkoppelingen.append(bestekkoppeling_new)

            else:
                logging.debug(f'Bestekkoppeling "{matching_koppeling.bestekRef.eDeltaDossiernummer}" bestaat al, '
                              f'status: {matching_koppeling.status.value}')

        if update_bestekkoppelingen:
            # Herorden de volgorde van de bestekkoppelingen: alle inactieve onderaan de lijst.
            bestekkoppelingen = sorted(bestekkoppelingen, key=lambda x: x.status.value, reverse=False)

            # Update all the bestekkoppelingen for this asset
            eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t',
                        filemode="w")
    logging.info('Bestekkoppelingen aanpassen voor assets van de Tunnels')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')
    eminfra_client = EMInfraClient(auth_type=AuthType.JWT, env=environment, settings_path=load_settings())

    # Read Excel as pandas dataframe
    filepath_input = Path.home() / 'Downloads' / 'MAR_TUNNEL' / BESTANDSNAAM
    df_assets = read_excel_as_df(filepath=filepath_input)

    process_assets(df=df_assets)
