from datetime import datetime

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import QueryDTO, ExpansionsDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, \
    OperatorEnum, LogicalOpEnum
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    # Some parameters to test the functions.
    environment = Environment.PRD
    print(f'environment:\t\t{environment}')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    oud_eDelta_dossiernummer = 'VWT/CEW/2020/009-3'
    nieuw_eDelta_dossiernummer = 'AWV/VW/2024/1_P3'

    bestekref1 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=oud_eDelta_dossiernummer)
    print(bestekref1)
    bestekref2 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=nieuw_eDelta_dossiernummer)
    print(bestekref2)

    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[
                                     TermDTO(property='actief', operator=OperatorEnum.EQ,
                                             value=True),
                                     TermDTO(property='actiefBestek', operator=OperatorEnum.EQ,
                                             value=bestekref1.uuid, logicalOp=LogicalOpEnum.AND)])]))
    for counter, asset in enumerate(eminfra_client.search_assets(query_dto)):
        print(f'Asset {counter + 1}: {asset.uuid}')
        if not asset.type.korteUri.startswith('lgc:'):
            print(f'Asset {counter + 1} is not a lgc type, skipped')
            continue

        eminfra_client.replace_bestekkoppeling(asset_uuid=asset.uuid, eDelta_dossiernummer_old=oud_eDelta_dossiernummer,
                                               eDelta_dossiernummer_new=nieuw_eDelta_dossiernummer,
                                               start_datetime=datetime(2025, 4, 1))





    #
    #
    #
    # # get_bestekkoppelingen_by_asset_uuid
    # bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid)
    # for index, p in enumerate(bestekkoppelingen):
    #     print(f'Bestekkoppeling {index + 1}: {p}')
    #
    # # get_bestekref_by_eDelta_dossiernummer
    # bestekref1 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)
    # print(bestekref1)

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
