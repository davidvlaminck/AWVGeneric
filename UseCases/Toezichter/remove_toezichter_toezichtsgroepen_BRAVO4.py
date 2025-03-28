from datetime import datetime

import pandas as pd
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


environment = Environment.PRD
print(f'environment:\t\t{environment}')
settings_path = Path.home() / 'OneDrive - Nordend' / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)
eindDatum = datetime(2025, 1, 1)

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

        generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='agents', filter_dict={"actief": True})
        agents = list(generator_agents)

        if not agents:
            pass
        elif len(agents) == 1:
            # todo loskoppelen toezichter
            # remove the toezichter
            print(f'Exactly 1 betrokkenerelaties (type: toezichter) found for asset: {asset_uuid}.')
            agent_uuid_lgc = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]

        elif len(agents) == 2:
            print(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')
            # raise ValueError(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')

        # todo idem for toezichtsgroep. Search and if exist, loskoppelen