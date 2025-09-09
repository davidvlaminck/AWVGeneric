from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd

from UseCases.utils import load_settings

if __name__ == '__main__':
    from pathlib import Path

    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path().home() / 'Downloads' / 'update_toezichters'

    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["otl_uuid", "lgc_uuid", "lgc_toezichthouder_voornaam", "lgc_toezichthouder_naam"])
    df_assets.drop_duplicates(inplace=True)

    for index, asset in df_assets.iterrows():
        asset_uuid_otl = asset['otl_uuid']
        asset_uuid_lgc = asset['lgc_uuid']
        lgc_toezichthouder_full_name = f'{asset["lgc_toezichthouder_voornaam"]} {asset["lgc_toezichthouder_naam"]}'
        print(f"Updating HeeftBetrokkene-relatie Toezichter for asset: {asset_uuid_otl}")

        #################################################################################
        ####  Get betrokkenerelatie from OTL-asset (rol=toezichter)
        #################################################################################
        generator_betrokkenerelaties = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='betrokkenerelaties', filter_dict={"bronAsset": asset_uuid_otl, 'rol': 'toezichter'})
        betrokkenerelaties = list(generator_betrokkenerelaties)
        if len(betrokkenerelaties) != 1:
            print(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid_otl}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')
            continue
            # raise ValueError(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid_otl}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')
        agent_uuid_otl = betrokkenerelaties[0].get('RelatieObject.doelAssetId').get('DtcIdentificator.identificator')[:36]   # agent_uuid (de persoon)
        betrokkenerelatie_uuid_otl = betrokkenerelaties[0].get('RelatieObject.assetId').get('DtcIdentificator.identificator')[:36]  # betrokkenerelatie_uuid (het relatieobject tussen een asset en een persoon)

        #################################################################################
        ####  Get agent from the LGC-asset
        #################################################################################
        generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='agents', filter_dict={"naam": lgc_toezichthouder_full_name})
        agents = list(generator_agents)
        if len(agents) != 1:
            print(f'Agent {lgc_toezichthouder_full_name} was not found or returned multiple results.')
            continue
            # raise ValueError(f'Agent {lgc_toezichthouder_full_name} was not found or returned multiple results.')
        agent_uuid_lgc = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]

        #################################################################################
        ####  Remove betrokkenerelatie toezichter from OTL-asset
        #################################################################################
        response = eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid_otl)

        #################################################################################
        ####  Add a new betrokkenerelatie - type: toezichter - to OTL-asset
        #################################################################################
        asset = eminfra_client.search_asset_by_uuid(asset_uuid=asset_uuid_otl)
        response = eminfra_client.add_betrokkenerelatie(asset=asset, agent_uuid=agent_uuid_lgc, rol='toezichter')
        betrokkenerelatie_uuid_otl_new = response.get('uuid')