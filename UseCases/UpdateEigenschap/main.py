from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.Enums import AuthType, Environment
import pandas as pd

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)
    daily_cookie = '' # to complete
    emson_client = EMSONClient(auth_type=AuthType.COOKIE, env=Environment.TEI, cookie=daily_cookie)

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / 'XXX.xlsx'

    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "naam", "naampad", "serienummer"])
    df_assets.drop_duplicates(inplace=True)

    for index, asset in df_assets.iterrows():
        asset_uuid = asset['uuid']
        print(f"Updating eigenschap 'serienummer' for asset: {asset_uuid}")

        ################################################################################
        ###  Get eigenschap
        ################################################################################
        # Eigenschap ophalen via API-call (EMSON)
        response = emson_client.get_asset_by_uuid(uuid=asset_uuid)
        serienummer_actual = response['lgc:SegC.serienummerSegmentController']

        # Convert serienummer to uppercase
        serienummer_new = serienummer_actual.upper()

        #################################################################################
        ####  Get eigenschap_uuid
        #################################################################################
        eigenschap_generator = eminfra_client.search_eigenschappen(eigenschap_naam='serienummer segment controller')
        if len(eigenschap_generator) != 1:
            raise ValueError('Multiple eigenschappen returned; Cannot extract the value of one single eigenschap')
        else:
            eigenschap_uuid = eigenschap_generator[0].uuid

        #################################################################################
        ####  Update eigenschap
        #################################################################################
        kenmerken = eminfra_client.get_kenmerken(assetId=asset_uuid)
        kenmerk_uuid = next(kenmerk.type.get('uuid') for kenmerk in kenmerken if kenmerk.type.get('naam').startswith('Eigenschappen'))
        eminfra_client.update_eigenschap(asset_uuid=asset_uuid, kenmerk_uuid=kenmerk_uuid, eigenschap_uuid=eigenschap_uuid, eigenschap_waarde=serienummer_new)