import json
import logging
from datetime import datetime

import pandas as pd
from pathlib import Path

from UseCases.utils import load_settings
from utils.date_helpers import format_datetime
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import BestekKoppelingStatusEnum, BestekCategorieEnum, BestekKoppeling
from API.Enums import AuthType, Environment


def read_excel_as_df(filepath: Path, usecols:list = None):
    if not usecols:
        usecols = [
        'uuid_prod',
        'naampad', '1e bestek', 'Startdatum 1e bestek', '2e bestek', 'Startdatum 2e bestek', '3e bestek',
        'Startdatum 3e bestek']

    df = pd.read_excel(filepath_input, sheet_name='Sheet0', header=0, usecols=usecols)
    df.rename(columns={
        # 'id_aim': 'uuid',  # deactivate environment aim or prod
        'id_prod': 'uuid',
        '1e bestek': 'bestek1_naam', 'Startdatum 1e bestek': 'bestek1_datum', '2e bestek': 'bestek2_naam',
        'Startdatum 2e bestek': 'bestek2_datum', '3e bestek': 'bestek3_naam',
        'Startdatum 3e bestek': 'bestek3_datum'}, inplace=True)
    return df


def process_assets(df: pd.DataFrame) -> list:
    """
    Toevoegen bestekkoppelingen voor een asset op basis van diens ID.
    Bestaande bestekkoppelingen blijven bewaard.
    Nieuwe bestekkoppelingen worden achteraan toegevoegd.
    Uiteindelijk worden alle bestekkoppelingen gesorteerd en de inactieve verschijnen achteraan.

    :param df:
    :return: a list of json-formatted data that can be written into a pandas dataframe.
    """
    rows = []
    for idx, row in df.iterrows():
        logging.debug(f'Processing asset ({idx + 1}/{len(df)}):\nnaampad: {row["naampad"]}')
        logging.debug(f'Search by asset_id: {row["uuid"]}')
        asset = eminfra_client.get_asset_by_id(asset_id=row["uuid"])
        if not asset:
            row.loc["uuid"] = None
            row = json.loads(row.to_json())
            rows.append(row)
            continue
        else:
            row.loc["uuid"] = asset.uuid
            row = json.loads(row.to_json())
            rows.append(row)

        # search all bestekkoppelingen
        bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset.uuid)

        # ophalen van de huidige/bestaande/actuele bestekkoppeling op basis van de naam.
        for i in range(1, 4):
            bestek_naam = row[f"bestek{i}_naam"]
            logging.debug('Skip empty values of Bestekken')
            if pd.isna(bestek_naam):
                continue
            bestek_datum = datetime.strptime(row[f"bestek{i}_datum"], "%d/%m/%Y %H:%M")
            logging.info(f'Appending bestek: {bestek_naam} with start_date: {bestek_datum}')

            bestekRef_new = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=bestek_naam)

            # Check if the new bestekkoppeling doesn't exist and append at the end of the list, else do nothing
            if not (matching_koppeling := next(
                    (k for k in bestekkoppelingen if k.bestekRef.uuid == bestekRef_new.uuid),
                    None, )):
                logging.debug(
                    f'Bestekkoppeling "{bestekRef_new.eDeltaBesteknummer}" bestaat nog niet, en wordt aangemaakt')
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

        # Herorden de volgorde van de bestekkoppelingen: alle inactieve onderaan de lijst.
        bestekkoppelingen = sorted(bestekkoppelingen, key=lambda x: x.status.value, reverse=False)

        # Update all the bestekkoppelingen for this asset
        eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset.uuid, bestekkoppelingen)

    return rows

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Bestekkoppelingen toevoegen voor assets van de Tijsmanstunnel')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')
    eminfra_client = EMInfraClient(auth_type=AuthType.JWT, env=environment, settings_path=load_settings())
    BESTANDSNAMEN = [
        'TYS.TUNNEL_met bestekken v Prod.xlsx'
        # , 'Import AIM_CRA_met bestekken v Prod.xlsx'
        # , 'Import AIM_RUP_met bestekken v Prod.xlsx'
        # , 'Import AIM_ZEL_met bestekken v Prod.xlsx'
        # , 'Import DEB_ZEL_met bestekken v Prod.xlsx'
    ]

    for bestandsnaam in BESTANDSNAMEN:
        # Read Excel as pandas dataframe
        filepath_input = Path.home() / 'Downloads' / 'Tunnels' / 'input_orig_bestanden_Patrick' / bestandsnaam
        df_assets = read_excel_as_df(filepath=filepath_input)
        filepath_output = Path.home() / 'Downloads' / 'Tunnels' / f'inspect_bestekken_{bestandsnaam}.xlsx'

        rows = process_assets(df=df_assets)

        logging.info(f"Write to output file: {filepath_output}")
        with pd.ExcelWriter(filepath_output, mode='w', engine='openpyxl') as writer:
            df_output = pd.DataFrame(data=rows)
            df_output.to_excel(writer,  freeze_panes=[1, 1], sheet_name='overzicht', index=False)