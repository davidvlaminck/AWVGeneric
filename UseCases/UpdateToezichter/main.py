from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path().home() / 'Downloads' / 'RSA Bijhorende assets hebben een verschillende toezichtshouder_toezichtsgroep (assettype = Signaalkabel).xlsx'

    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, nrows=5)
    otl_uuids = df_assets.loc[:, 'otl_uuid']
    lgc_uuids = df_assets.loc[:, 'lgc_uuid']

    # Get all betrokkenerelaties.
    sample_asset = df_assets.iloc[0, :]
    asset_uuid_otl = sample_asset['otl_uuid']
    asset_uuid_lgc = sample_asset['lgc_uuid']

    #################################################################################
    ####  Get betrokkenerelatie from OTL-asset (rol=toezichter)
    #################################################################################

    generator_betrokkenerelaties = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='betrokkenerelaties', filter_string={"bronAsset": asset_uuid_otl, 'rol': 'toezichter'})

    betrokkenerelaties = list(generator_betrokkenerelaties)

    if len(betrokkenerelaties) != 1:
        raise ValueError(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid_otl}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')

    agent_uuid_otl = betrokkenerelaties[0].get('RelatieObject.doelAssetId').get('DtcIdentificator.identificator')[:36]   # agent_uuid (de persoon)
    betrokkenerelatie_uuid_otl = betrokkenerelaties[0].get('RelatieObject.assetId').get('DtcIdentificator.identificator')[:36]  # betrokkenerelatie_uuid (het relatieobject tussen een asset en een persoon)

    #################################################################################
    ####  Get agent from the LGC-asset
    #################################################################################
    lgc_toezichthouder = f'{sample_asset["lgc_toezichthouder_voornaam"]} {sample_asset["lgc_toezichthouder_naam"]}'

    generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='agents', filter_string={"naam": lgc_toezichthouder})
    agents = list(generator_agents)

    if len(agents) != 1:
        raise ValueError(f'There are 2 or more agents  found for the name: {lgc_toezichthouder}')
    agent_uuid_lgc = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]

    #################################################################################
    ####  Add a new betrokkenerelatie - type: toezichter - to OTL-asset
    #################################################################################
    response = eminfra_client.add_betrokkenerelatie(asset_uuid=asset_uuid_otl, agent_uuid=agent_uuid_lgc)
    betrokkenerelatie_uuid_otl_new = response.get('uuid')

    #################################################################################
    ####  Remove betrokkenerelatie toezichter from OTL-asset
    #################################################################################
    response = eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid_otl)