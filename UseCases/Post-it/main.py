from datetime import datetime
from pathlib import Path

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':

    # Some parameters to test the functions.
    asset_uuid = '00000453-56ce-4f8b-af44-960df526cb30' # random kast
    environment = Environment.TEI
    print(f'environment:\t\t{environment}')
    print(f'asset_uuid:\t\t{asset_uuid}')

    settings_path = Path.home() / 'OneDrive - Nordend' / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # search_postits (all postits)
    postits_generator = eminfra_client.search_postits(asset_uuid=asset_uuid)
    postit_uuid = list(postits_generator)[-1].uuid # postit that was returned last in the call to serach for all postits.

    # get_postit (one single postit)
    postit_generator = eminfra_client.get_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid)

    # add_postit
    response = eminfra_client.add_postit(asset_uuid=asset_uuid, commentaar='test add postit using REST API in Python', startDatum=datetime(2025,2,1), eindDatum=datetime(2025, 2, 28))

    # edit_postit
    response = eminfra_client.edit_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid, commentaar='new comment for a same postit', startDatum=datetime(2025,2,13), eindDatum=datetime(2025,2,14))

    # edit_postit (soft delete: set eindDatum on today's date)
    response = eminfra_client.edit_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid, eindDatum=datetime(2025,2,28))

    # remove postit (hard delete)
    response = eminfra_client.remove_postit(asset_uuid=asset_uuid, postit_uuid=postit_uuid)