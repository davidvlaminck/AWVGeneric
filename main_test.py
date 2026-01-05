from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import  QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum
from API.eminfra.AssetService import AssetService
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # asset_types = eminfra_client.get_all_legacy_assettypes()
    # print(list(asset_types))

    query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='type',
                                                operator=OperatorEnum.EQ,
                                                value='a7eadedf-b5cf-491b-8b89-ccced9a37004')])]))
    asset = list(AssetService.search_assets_gen(query_dto, ))
    print(asset)

    # bestekken = eminfra_client.get_bestekkoppelingen_by_asset_uuid('030a47c0-bf19-434a-aa19-e33377c82f79')
    # print(bestekken)

    # bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer('MDN/67-5')
    # print(bestek_ref)
    # #
    # feedproxy_page = eminfra_client.get_feedproxy_page('assets', 0)
    # print(feedproxy_page)

    # emson_client = EMSONClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=settings_path)
    # # asset = emson_client.get_asset_by_uuid('030a47c0-bf19-434a-aa19-e33377c82f79')
    # # print(asset)
    #
    # assets = emson_client.get_assets_by_filter(
    #     filter={'typeUri': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Motorvangplank'}, size=100)
    # print(list(assets))
    #
    # relaties = emson_client.get_assetrelaties_by_filter(
    #     filter={'asset': '000022d5-8e1d-419a-aceb-7cfd1749aed6'})
    # print(list(relaties))
