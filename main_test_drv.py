from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum
from API.EMSONClient import EMSONClient
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # asset_uuid = '001c1e9c-f1ff-478a-96df-42d845f4e9aa'
    # documents = eminfra_client.get_documents_by_asset_uuid(asset_uuid=asset_uuid)
    #
    # for i, document in enumerate(documents):
    #     print(f'Document number: {i}')
    #     print(document)
    #     document_link = document.document['links'][0]['href']
    #     print(f'document link: {document_link}')

    # asset_types = eminfra_client.get_all_legacy_assettypes()
    # print(list(asset_types))

    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer('VWT/DVM/2023/3')
    print(bestek_ref)

    # TODO test de API Call POST. Deze levert voorlopig nog geen resultaten op.
    bestek_ref_uuid = bestek_ref[0].uuid
    query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.CURSOR,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='actiefBestek',
                                                # operator=OperatorEnum.EQ,
                                                operator=OperatorEnum.EQ1,
                                                value='bestek_ref_uuid')])]))
    asset = list(eminfra_client.search_assets(query_dto))
    print(asset)

    # bestekken = eminfra_client.get_bestekkoppelingen_by_asset_uuid('030a47c0-bf19-434a-aa19-e33377c82f79')
    # print(bestekken)

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
