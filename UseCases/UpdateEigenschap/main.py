from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.Enums import AuthType, Environment
import pandas as pd

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)
    # Authenticatie methode JWT werkt niet met de Nordend-token > gebruik de cookie
    # emson_client = EMSONClient(auth_type=AuthType.JWT, env=Environment.PRD, settings_path=settings_path)
    emson_client = EMSONClient(auth_type=AuthType.COOKIE, env=Environment.PRD, cookie='')

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / 'XXX.xlsx'

    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "naam", "naampad", "serienummer"])
    df_assets.drop_duplicates(inplace=True)

    for index, asset in df_assets.iterrows():
        asset_uuid = asset['uuid']
        print(f"Updating eigenschap 'serienummer' for asset: {asset_uuid}")

        #################################################################################
        ####  Get eigenschap
        #################################################################################
        # Eigenschap ophalen via Excel of via API-call (EMSON)
        response = emson_client.get_asset_by_uuid(uuid=asset_uuid)
        serienummer_actual = response['lgc:SegC.serienummerSegmentController']

        # Convert serienummer to uppercase
        serienummer_new = serienummer_actual.upper()

        #################################################################################
        ####  Update eigenschap
        #################################################################################
        eigenschap_uuid = 'ce1d97ff-40bb-47b3-ac27-b491c9c52e71'  ## serienummer
        eminfra_client.update_eigenschap(asset_uuid=asset_uuid, eigenschap_uuid=eigenschap_uuid, eigenschap_waarde=serienummer_new)