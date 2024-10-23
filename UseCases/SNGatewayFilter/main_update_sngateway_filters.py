from pathlib import Path
from prettytable import PrettyTable

from API.RequesterFactory import RequesterFactory
from API.SNGatewayClient import SNGatewayClient
from API.Enums import AuthType, Environment


awv_acm_cookie = '55eb0c4eef5d41debd0d639c4db7e37c'
voId = '6c2b7c0a-11a9-443a-a96b-a1bec249c629'  # zie https://apps.mow.vlaanderen.be/eminfra/admin/gebruikers

if __name__ == '__main__':
    #requester = RequesterFactory.create_requester(cookie=awv_acm_cookie, auth_type=AuthType.COOKIE, env=Environment.PRD)
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    requester = RequesterFactory.create_requester(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.TEI)
    sn_client = SNGatewayClient(requester=requester)

    # sn_client.add_new_asset_filter(uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Motorvangplank', enabled=True)

    json_data = sn_client.get_all_asset_filters()
    headers = ['id', 'version', 'created', 'updated', 'uri', 'enabled']
    rows = [[row.get(h, '') for h in headers] for row in sorted(json_data, key=lambda x: x['updated'], reverse=True)]

    table = PrettyTable(headers)
    table.add_rows(rows)
    print(table)
