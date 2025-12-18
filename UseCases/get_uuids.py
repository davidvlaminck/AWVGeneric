import logging
from pathlib import Path

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger

if __name__ == '__main__':
    configure_logger()
    logging.info('Read an Excel and get the UUID.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    filepath = Path.home() / 'downloads' / 'netwerkpoorten.xlsx'
    if not Path.exists(filepath):
        raise FileNotFoundError(f'Filepath does not exists {filepath}.')
    df_assets = pd.read_excel(filepath, sheet_name='Sheet1', header=0, usecols=["asset_naam", "uuid"])

    for idx, df_row in df_assets.iterrows():
        asset_naam = df_row["asset_naam"]
        logging.info(f"Processing asset: ({idx + 1}/{len(df_assets)}): asset_name: {asset_naam}")
        if asset := next(eminfra_client.search_asset_by_name(asset_name=asset_naam, exact_search=True), None):
            df_assets.at[idx, "uuid"] = asset.uuid

    # Append to existing file
    with pd.ExcelWriter(filepath, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_assets.to_excel(writer, sheet_name='Netwerkpoorten', index=False, freeze_panes=[1, 1])
