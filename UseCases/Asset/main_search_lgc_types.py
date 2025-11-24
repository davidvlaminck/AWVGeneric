from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documenten/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    asset_types = list(eminfra_client.get_all_legacy_assettypes())
    print(f'aantal legacy types: {len(asset_types)}')

    for asset_type in asset_types:
        print(asset_type.uri)
