from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, LogicalOpEnum
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    asset_uuid = '05b1d72d-d707-4514-9650-b882e8baac78'
    environment = Environment.TEI
    eDelta_dossiernummer = 'INTERN-025'
    eDelta_besteknummer = 'AANNEMER HBA DVM'
    print(f'environment:\t\t{environment}')
    print(f'asset_uuid:\t\t{asset_uuid}')
    print(f'eDelta_dossiernummer:\t\t{eDelta_dossiernummer}')
    print(f'eDelta_besteknummer:\t\t{eDelta_besteknummer}')

    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    # todo schrijf eerst de functies in dit main-script en verplaats nadien naar de EMInfraClient file
    # get_bestekkoppelingen_by_asset_uuid
    bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid)
    print(bestekkoppelingen)

    # get_bestekref_by_eDelta_dossiernummer
    bestekref1 = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)[0]

    # get_bestekref_by_eDelta_besteknummer (new)
    # bestekref2 = eminfra_client.get_bestekref_by_eDelta_besteknummer(eDelta_besteknummer=eDelta_besteknummer)

    # change_bestekkoppelingen_by_asset_uuid (new)
    # eminfra_client.change_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid, bestekkoppeling=bestekkoppelingen)[0]

    # adjust_date_bestekkoppeling (new). Schuif de stardatum (optioneel) en einddatum (optioneel) op.
    # eminfra_client.adjust_date_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref1.uuid, start_date='2022-07-27', end_date='2025-01-31')

    # end_bestekkoppeling (new)
    # eminfra_client.end_bestekkoppeling(asset_uuid=asset_uuid, bestek_ref_uuid=bestekref1.uuid, end_date='2025-01-30')

    # todo tot hier
    # add_bestekkoppeling (new)
    eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer=eDelta_dossiernummer)

    # update_bestekkoppeling (new)


