import json
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import Environment, AuthType


if __name__ == '__main__':
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    generator_assets = eminfra_client.get_objects_from_oslo_search_endpoint(url_part='assets', filter_string={
        "typeUri": 'https://lgc.data.wegenenverkeer.be/ns/installatie#SeinbrugDVM',
        # uri uit https://apps.mow.vlaanderen.be/eminfra/admin/installatietypes/6f66dad8-8290-4d07-8e8b-6add6c7fe723
        'actief': True})

    print('Downloading...')
    all_assets = list(generator_assets)
    with open(Path('seinbruggen.json'), 'w') as json_file:
        json.dump(all_assets, json_file, indent=4)
    print('Done.')