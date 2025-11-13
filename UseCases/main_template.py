import logging

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report, configure_logger

if __name__ == '__main__':
    configure_logger()
    logging.info('Use case naam:\t use case beschrijving')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    df_assets = read_rsa_report()

    rows = []
    for _, asset in df_assets.iterrows():
        print("Implement function logic here")
        row = {
            "uuid": ''
        }
        rows.append(row)

    output_excel_path = 'test_output.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])