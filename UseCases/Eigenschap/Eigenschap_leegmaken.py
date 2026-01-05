from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import AssetDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

print(""""
        Wissen (leegmaken) van de wetenschappelijke notatie van een attribuut, door het toekennen van een lege string.      
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def read_report():
    """Read RSA-report as input into a DataFrame."""
    filepath = Path().home() / 'Downloads' / 'update_eigenschap' / '[RSA] Wetenschappelijke notaties komen niet voor.xlsx'
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "attribuutnaam"])
    return df_assets

def update_asset(asset: AssetDTO, eminfra_client: EMInfraClient):
    """Process and update a single asset."""
    asset_uuid = asset['uuid']
    print(f"Updating asset: {asset_uuid}\teigenschap '{asset.attribuutnaam}'")

    eigenschapwaarden = eminfra_client.get_eigenschapwaarden(assetId=asset_uuid)
    eigenschap = next(item for item in eigenschapwaarden if item.eigenschap.naam == asset.attribuutnaam)

    if not eigenschap:
        raise ValueError(f"Eigenschap '{asset.attribuutnaam}' niet gevonden voor asset {asset_uuid}")

    value = eigenschap.typedValue['value'].upper()
    if "E+" in value:
        eigenschap.typedValue['value'] = None
    else:
        raise ValueError("Eigenschap waarde bevat geen wetenschappelijke notatie")

    eminfra_client.update_eigenschap(assetId=asset_uuid, eigenschap=eigenschap)

if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = read_report()

    for _, asset in df_assets.iterrows():
        update_asset(asset, eminfra_client)