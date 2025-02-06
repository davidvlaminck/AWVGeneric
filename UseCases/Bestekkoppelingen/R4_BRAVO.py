import glob
import os
from datetime import datetime

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

start_datetime = datetime(2025, 1, 1)
eDelta_dossiernummer_new = 'INTERN-5904'

print("Update de bestekkoppelingen voor assets die gelinkt zijn aan het project BRAVO4.")
print("Beëindig het huidige bestek en installeer een nieuw bestek: INTERN-5904")
print(f"De einddatum van het vorige bestek is tevens de startdatum van het nieuwe bestek: {start_datetime}")

environment = Environment.PRD
settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)
print(f"Initializing an EMInfraClient on the {environment} environment")

print("Read the Excel input files.")
print("Convert to a pandas dataframe and apply conditionaly filtering on 'actief'='ja' and 'toestand'='overgedragen'.")
root_folder = Path().home() / 'Downloads' / 'installatiesvoorbravo4'
xlsx_files = glob.glob(os.path.join(root_folder, "*.xlsx"))

for filepath in xlsx_files:
    print(f"Opening file: {filepath}")

    df_assets = pd.read_excel(filepath, sheet_name='Sheet0', header=0,
                              usecols=["id", "actief", "toestand", "bestek|edelta dossiernummer"])
    df_assets_overgedragen = df_assets[(df_assets['actief'] == 'ja') & (df_assets['toestand'] == 'OVERGEDRAGEN') & (df_assets['bestek|edelta dossiernummer'] != 'INTERN-5904')]
    df_assets_overgedragen.rename(columns={"bestek|edelta dossiernummer": "eDelta_dossiernummer"}, inplace=True)

    print("\tStart updating bestekkoppelingen")
    for index, asset in df_assets_overgedragen.iterrows():
        if not asset["id"]:
            raise ValueError('An asset_uuid is missing')
        else:
            asset_uuid = asset["id"]

        # end_bestekkoppeling
        eDelta_dossiernummer_old = asset["eDelta_dossiernummer"]
        if eDelta_dossiernummer_old is not None and pd.notna(eDelta_dossiernummer_old):
            print(f'End the actual bestekkoppeling for eDeltadossiernummer {eDelta_dossiernummer_old}, assigned to asset: {asset_uuid}')
            bestek_ref_uuid = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer_old)[0].uuid
            eminfra_client.end_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestek_ref_uuid, end_datetime=start_datetime)

        # add_bestekkoppeling
        eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer=eDelta_dossiernummer_new, start_datetime=start_datetime)

    print("\tEnd updating bestekkoppelingen")
