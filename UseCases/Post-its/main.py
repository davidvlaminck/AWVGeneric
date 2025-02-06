import os
from datetime import datetime
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':

    # Some parameters to test the functions.
    asset_uuid = '' # random kast
    environment = Environment.TEI
    print(f'environment:\t\t{environment}')
    print(f'asset_uuid:\t\t{asset_uuid}')

    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # search_postits
    # todo tot hier
    f'https://apps-tei.mow.vlaanderen.be/eminfra/core/api/assets/{asset_uuid}/postits/search'
    # post
    # {"size":10,"from":0,"selection":{"expressions":[{"terms":[{"property":"eindDatum","value":"2025-02-06T15:26:50.260+01:00","operator":3,"logicalOp":null,"negate":false}],"logicalOp":null}],"settings":{}},"expansions":{"fields":[]},"pagingMode":"OFFSET","orderByProperty":"eindDatum","orderByDirection":"DESC"}

    # get_postits
    # edit_postits
