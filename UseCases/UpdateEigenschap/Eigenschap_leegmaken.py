from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd

print(""""
        Wissen (leegmaken) van de wetenschappelijke notatie van een attribuut, door het toekennen van een lege string.      
      """)

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / '[RSA] Wetenschappelijke notaties komen niet voor.xlsx'
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "attribuutnaam"])

    ################################################################################
    ###  Update asset eigenschappen
    ################################################################################
    for index, asset in df_assets.iterrows():
        asset_uuid = asset['uuid']
        print(f"Updating asset: {asset_uuid}\teigenschap '{asset.attribuutnaam}'")

        ################################################################################
        ###  Get eigenschapwaarden
        ################################################################################
        eigenschapwaarden = eminfra_client.get_eigenschapwaarden(assetId=asset_uuid)
        eigenschap = next(item for item in eigenschapwaarden if item.eigenschap.naam == asset.attribuutnaam)

        #################################################################################
        ####  Update eigenschap value
        #################################################################################
        if not eigenschap:
            raise ValueError(f"Eigenschap '{asset.attribuutnaam}' niet gevonden voor asset {asset_uuid}")

        value = eigenschap.typedValue['value'].upper()
        if "E+" in value:
            eigenschap.typedValue['value'] = None
        else:
            raise ValueError("Eigenschap waarde bevat geen wetenschappelijke notatie")

        #################################################################################
        ####  Update eigenschap
        #################################################################################
        eminfra_client.update_eigenschap(assetId=asset_uuid, eigenschap=eigenschap)