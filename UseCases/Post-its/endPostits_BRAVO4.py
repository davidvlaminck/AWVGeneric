from datetime import datetime

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


environment = Environment.PRD
print(f'environment:\t\t{environment}')
settings_path = Path.home() / 'OneDrive - Nordend' / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)
eindDatum = datetime(2025, 2, 1)

print("Read the Excel input files.")
print("Convert to a pandas dataframe and apply conditionaly filtering on 'actief'='ja' and 'toestand'='overgedragen'.")
root_folder = Path().home() / 'Downloads' / 'installatiesvoorbravo4' / 'te_verwerken'
xlsx_files = list(root_folder.glob(pattern="*.xlsx"))

for filepath in xlsx_files:
    print(f"Opening file: {filepath}")
    df_assets = pd.read_excel(filepath, sheet_name='Sheet0', header=0,
                              usecols=["id", "actief", "toestand", "bestek|edelta dossiernummer"])
    df_assets_overgedragen = df_assets[(df_assets['actief'] == 'ja') & (df_assets['toestand'] == 'OVERGEDRAGEN') & (df_assets['bestek|edelta dossiernummer'] != 'INTERN-5904')]
    df_assets_overgedragen.rename(columns={"bestek|edelta dossiernummer": "eDelta_dossiernummer"}, inplace=True)

    for index, asset in df_assets_overgedragen.iterrows():
        if not asset["id"]:
            raise ValueError('An asset_uuid is missing')
        else:
            asset_uuid = asset["id"]

        # search_postits (all postits)
        postits_generator = eminfra_client.search_postits(asset_uuid=asset_uuid)
        postit_uuids = list(postits_generator)

        if len(postit_uuids) == 0:
            pass
        elif len(postit_uuids) != 1:
            print(f"Multiple postits found for asset: {asset_uuid}")
        else:
            print(f'Found exactly 1 postit for asset: {asset_uuid}')

            # edit_postit (soft delete: set eindDatum on today's date)
            postit_uuid = postit_uuids[0].uuid
            response = eminfra_client.edit_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid, eindDatum=eindDatum)