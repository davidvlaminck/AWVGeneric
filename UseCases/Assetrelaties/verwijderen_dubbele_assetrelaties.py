import logging
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import RelatieEnum
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report, configure_logger

if __name__ == '__main__':
    configure_logger()
    logging.info('Verwijderen van niet-gerichte assetsrelaties die dubbel voorkomen.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    input_excel_path = Path.home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0211' / '[RSA] Dubbele niet-gerichte relatie tussen assets.xlsx'
    df_assets = read_rsa_report(filepath=input_excel_path, usecols=['bronuuid', 'doeluuid', 'relatie', 'unique'])
    logging.info('Filter op unieke records: kolom unique = False')
    df_assets = df_assets[df_assets['unique'] == False]

    rows = []
    for idx, df_row in df_assets.iterrows():
        bronasset_uuid = df_row["bronuuid"]
        doelasset_uuid = df_row["doeluuid"]
        relatie = df_row["relatie"]
        logging.info(f"Processing asset: ({idx + 1}/{len(df_assets)}): asset_uuid: {bronasset_uuid}")
        if relatie == 'Bevestiging':
            rel = RelatieEnum.BEVESTIGING
        elif relatie == 'Sturing':
            rel = RelatieEnum.STURING
        else:
            raise ValueError(f'Relatie {relatie} not found.')
        eminfra_client.remove_assets_via_relatie(bronasset_uuid=bronasset_uuid, doelasset_uuid=doelasset_uuid, relatie=rel)
        row = {
            "bronasset_uuid": bronasset_uuid,
            "doelasset_uuid": doelasset_uuid
        }
        rows.append(row)

    output_excel_path = 'assets_relaties_verwijderd.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])