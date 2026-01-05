import os
from datetime import datetime

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    # Some parameters to test the functions.
    asset_uuid = ''
    environment = Environment.TEI
    eDelta_dossiernummer = ''
    eDelta_besteknummer = ''
    print(f'environment:\t\t{environment}')
    print(f'asset_uuid:\t\t{asset_uuid}')
    print(f'eDelta_dossiernummer:\t\t{eDelta_dossiernummer}')
    print(f'eDelta_besteknummer:\t\t{eDelta_besteknummer}')

    settings_path = Path(os.environ["OneDrive"]) / 'projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    dummyAsset = eminfra_client.assets.get_asset(asset_uuid=asset_uuid)

    # get_bestekkoppelingen_by_asset_uuid
    bestekkoppelingen = eminfra_client.bestekken.get_bestekkoppeling(asset=dummyAsset)

    # get_bestekref_by_eDelta_dossiernummer
    bestekref1 = eminfra_client.bestekken.get_bestekref(eDelta_dossiernummer=eDelta_dossiernummer)

    # get_bestekref_by_eDelta_besteknummer (new)
    bestekref2 = eminfra_client.bestekken.get_bestekref(eDelta_besteknummer=eDelta_besteknummer)

    # change_bestekkoppelingen_by_asset_uuid (new)
    eminfra_client.bestekken.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid, bestekkoppelingen=bestekkoppelingen)

    # adjust_date_bestekkoppeling (new). Schuif de stardatum (optioneel) en einddatum (optioneel) op.
    eminfra_client.adjust_date_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref1.uuid,
                                               start_datetime=datetime(2022, 10, 29), end_datetime=datetime(2026, 10, 28))

    # end_bestekkoppeling (new)
    eminfra_client.end_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref1.uuid)

    # add_bestekkoppeling (new)
    eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer='INTERN-002',
                                       start_datetime=datetime(2025, 1, 29), end_datetime=datetime(2025, 2, 1))

    # update_bestekkoppeling (new)
    eminfra_client.replace_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer_old='INTERN-002',
                                           eDelta_dossiernummer_new='INTERN-003')
