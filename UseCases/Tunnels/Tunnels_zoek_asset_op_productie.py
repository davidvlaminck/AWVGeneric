import logging
from pathlib import Path

import pandas as pd

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger

INPUT_DIR = Path.home() / 'Downloads/Tunnels/input_preprocessing'
INPUT_FILES = [
    'Import AIM_RUP_met bestekken v Prod_20251105.xlsx'
    , 'Import AIM_ZEL_met bestekken v Prod_20251105.xlsx'
    , 'Import AIM_CRA_met bestekken v Prod_20251105.xlsx'
    , 'Import DEB_ZEL_met bestekken v Prod_20251105.xlsx'
]
INVALID_ASSETTYPES = [
            'lgc:installatie#AID', 'lgc:installatie#ANPR', 'lgc:installatie#CCTV', 'lgc:installatie#PTZ',
            'lgc:installatie#CamGroep', 'lgc:installatie#Decoder', 'lgc:installatie#Encoder',
            'lgc:installatie#OmvormerLegacy']

def read_input_file(filepath: Path, sheet_name: str = 'Sheet0', header: int = 0, usecols: list = None) -> pd.DataFrame():
    if usecols is None:
        usecols = ['uuid_aim', 'uuid_prod', 'naampad', 'type']
    return pd.read_excel(filepath, sheet_name=sheet_name, header=header, usecols=usecols)


def validate_assettypes(df: pd.DataFrame, column_assettype:str = 'type', invalid_assettypes: list = None):
    """
    Validates if a forbidden assettype is present in a specific column of the dataframe.

    :param df:
    :param column_assettype:
    :param invalid_assettypes:
    :return:
    """
    if invalid_assettypes is None:
        invalid_assettypes = [
            'lgc:installatie#AID', 'lgc:installatie#ANPR', 'lgc:installatie#CCTV', 'lgc:installatie#PTZ',
            'lgc:installatie#CamGroep', 'lgc:installatie#Decoder', 'lgc:installatie#Encoder',
            'lgc:installatie#OmvormerLegacy']
    df_invalid_assettypes = df[df[column_assettype].isin(invalid_assettypes)]
    if not df_invalid_assettypes.empty:
        raise ValueError(f'Input file contains one or more invalid asset types: {invalid_assettypes}')
    else:
        return None


def get_input_files() -> list[Path]:
    paths = [INPUT_DIR / f for f in INPUT_FILES]
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(f"{p} not found")
    return paths

if __name__ == '__main__':
    configure_logger()
    logging.info('Tunnels: ')
    logging.info('Controleren of een ID van een bestaand asset uit de tunnelboom uit AIM, eveneens op de Productieomgeving bestaat.')
    logging.info('Kopiëren van dit ID of leeglaten indien het asset nog niet bestaat')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    for input_file in get_input_files():
        df_tunnel_assets = read_input_file(input_file)
        validate_assettypes(df=df_tunnel_assets)

        rows = []
        for idx, df_row in df_tunnel_assets.iterrows():
            uuid_aim = df_row["uuid_aim"]
            row = {
                "uuid_aim": uuid_aim,
                "uuid_prd": None,
                "naampad": df_row["naampad"],
                "type": df_row["type"],
            }
            logging.info(f'Processing ({idx+1}/{len(df_tunnel_assets)}): {uuid_aim}.')

            try:
                asset = eminfra_client.get_asset_by_id(asset_id=uuid_aim)
                row["uuid_prd"] = asset.uuid
            except Exception as e:
                logging.debug(f'Exception occurred: {e}.')
                logging.info(f'Overeenkomstig asset {uuid_aim} bestaat (nog) niet op productie.')
                asset = None

            rows.append(row)

        with pd.ExcelWriter(input_file, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='lookup', index=False, freeze_panes=[1, 1])