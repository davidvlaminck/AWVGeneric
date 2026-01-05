from pathlib import Path

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import Environment, AuthType

if __name__ == '__main__':
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    with open('schema.json', 'w', newline='') as file:
        schema = eminfra_client.get_oef_schema_as_json('legacy')
        file.write(schema)
