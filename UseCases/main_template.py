import logging

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report, configure_logger

if __name__ == '__main__':
    configure_logger()
    logging.info('Use case naam:\t use case beschrijving')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    df_assets = read_rsa_report()

    rows = []
    for idx, df_row in df_assets.iterrows():
        asset_uuid = df_row["id"]
        logging.info(f"Processing asset: ({idx + 1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
        asset = eminfra_client.get_asset_by_id(asset_id=asset_uuid)
        row = {
            "uuid": ''
        }
        rows.append(row)

    output_excel_path = 'test_output.xlsx'
    # Append to existing file
    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])
    # Write to a new file
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])