from API.EMInfraClient import EMInfraClient
from API.RequesterFactory import RequesterFactory
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    requester = RequesterFactory.create_requester(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.TEI)
    sn_client = EMInfraClient(requester=requester)
    bestekken = sn_client.get_bestekkoppelingen_by_asset_uuid('030a47c0-bf19-434a-aa19-e33377c82f79')
    print(bestekken)
