import json
from pathlib import Path

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import Environment, AuthType

if __name__ == '__main__':
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    with open('attributes.json', 'w', newline='') as file:
        attributes = eminfra_client.get_all_eigenschappen_as_text_generator()
        json.dump(list(attributes), fp=file, indent=4)
