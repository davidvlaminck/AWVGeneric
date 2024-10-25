from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # bestekken = eminfra_client.get_bestekkoppelingen_by_asset_uuid('030a47c0-bf19-434a-aa19-e33377c82f79')
    # print(bestekken)

    # bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer('MDN/67-5')
    # print(bestek_ref)
    # #
    # feedproxy_page = eminfra_client.get_feedproxy_page('assets', 0)
    # print(feedproxy_page)

    emson_client = EMSONClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)
    asset = emson_client.get_asset_by_uuid('030a47c0-bf19-434a-aa19-e33377c82f79')
    print(asset)

    assets = emson_client.get_assets_by_filter(
        filter={'uri': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Motorvangplank'}, size=10)
    print(assets)