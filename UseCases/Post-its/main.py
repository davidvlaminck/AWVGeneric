import os
from datetime import datetime
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum
from API.Enums import AuthType, Environment


if __name__ == '__main__':

    # Some parameters to test the functions.
    asset_uuid = '00000453-56ce-4f8b-af44-960df526cb30' # random kast
    environment = Environment.TEI
    print(f'environment:\t\t{environment}')
    print(f'asset_uuid:\t\t{asset_uuid}')

    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # search_postits
    postits_generator = eminfra_client.search_postits(asset_uuid=asset_uuid)
    postits_generator_list = list(postits_generator)

    # todo tot hier
    # get (is dit overbodig, want we hebben misschien hetzelfde resultaat met de search post?)

    # make_postits (verplicht start en einddatum)
    # POST
    # /assets/{id}/postits

    # edit_postits

    # PUT
    # /assets/{id}/postits/{id}
