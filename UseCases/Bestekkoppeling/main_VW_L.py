import os
from datetime import datetime

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    # Some parameters to test the functions.
    asset_uuid = '000d450f-157a-42b6-acb2-a303a3226110'
    environment = Environment.PRD
    eDelta_dossiernummer = 'VWT/CEW/2020/009-2'

    print(f'environment:\t\t{environment}')
    print(f'asset_uuid:\t\t{asset_uuid}')
    print(f'eDelta_dossiernummer:\t\t{eDelta_dossiernummer}')


    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # get_bestekkoppelingen_by_asset_uuid
    bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid)
    for index, p in enumerate(bestekkoppelingen):
        print(f'Bestekkoppeling {index + 1}: {p}')

    # get_bestekref_by_eDelta_dossiernummer
    bestekref1 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)
    print(bestekref1)

    # in bestekkopelingen, the koppeling with eDelta_dossiernummer = 'VWT/CEW/2020/009-2' needs to get an end_date of
    # 2025-04-02 and add a new koppeling (as first) with eDelta_dossiernummer = 'AWV/VW/2024/1_P2' with start_date
    # 2025-04-02






    # # change_bestekkoppelingen_by_asset_uuid (new)
    # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid, bestekkoppelingen=bestekkoppelingen)
    #
    # # adjust_date_bestekkoppeling (new). Schuif de stardatum (optioneel) en einddatum (optioneel) op.
    # eminfra_client.adjust_date_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref1.uuid,
    #                                            start_datetime=datetime(2022, 10, 29), end_datetime=datetime(2026, 10, 28))
    #
    # # end_bestekkoppeling (new)
    # eminfra_client.end_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref1.uuid)
    #
    # # add_bestekkoppeling (new)
    # eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer='INTERN-002',
    #                                    start_datetime=datetime(2025, 1, 29), end_datetime=datetime(2025, 2, 1))
    #
    # # update_bestekkoppeling (new)
    # eminfra_client.replace_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer_old='INTERN-002',
    #                                        eDelta_dossiernummer_new='INTERN-003')
