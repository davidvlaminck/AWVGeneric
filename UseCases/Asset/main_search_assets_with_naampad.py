from prettytable import PrettyTable

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO, construct_naampad
from API.Enums import AuthType, Environment

# requires prettytable, requests, pyjwt

if __name__ == '__main__':
    from pathlib import Path

    cookie = "99a76f29daf6498594d9c27e5c611fb3"
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.COOKIE, cookie=cookie)

    verweving_white_list = ['https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel/#Bel',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Slagboom',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Hulppost',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#InwendigVerlichtPictogram',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Brandblusser',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#LuchtkwaliteitZenderOntvanger',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Luchtkwaliteitsensor',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Noodverlichtingstoestel',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IntercomToestel',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IntercomServer',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Contourverlichting',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Geleidingsverlichting',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Kokerventilatie',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Ventilatiecluster',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Ventilator',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel/#Pomp',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Pompstation',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Gassensor',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtsensor',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Luchtkwaliteitsensor',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Tunnelverlichting',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkelement',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Z30Groep',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordZ30',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordPK',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Hoogtedetectie',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#ASTRIDInstallatie',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#RadioheruitzendInstallatie',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Datakabel',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ITSapp',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Meteostation',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Elektromotor',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Toegangscontroller',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVMeetpunt',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Fietstelinstallatie',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#FietstelDisplay',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Fietstelsysteem',
    'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Zoutbijlaadplaats',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Tank',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Silo',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordExternePU',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#StralendeKabel',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#NietSelectieveDetectielus',
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Cabine']

    asset_types = list(eminfra_client.get_all_otl_assettypes())
    total_types = len(asset_types)
    print(f'aantal OTL types: {total_types}')

    type_term = TermDTO(property='type', operator=OperatorEnum.EQ, value='a7eadedf-b5cf-491b-8b89-ccced9a37004')
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[type_term,
                                     TermDTO(property='actief', operator=OperatorEnum.EQ,
                                             value=True, logicalOp=LogicalOpEnum.AND),
                                     TermDTO(property='beheerobject', operator=OperatorEnum.EQ,
                                             value=None, logicalOp=LogicalOpEnum.AND, negate=True)])]))
    headers = ['uuid', 'type', 'naampad', 'em_infra_link']
    rows = []
    for index, otl_asset_type in enumerate(asset_types):
        if otl_asset_type.uri in verweving_white_list:
            print(f'{index+1}/{total_types} skipping type {otl_asset_type.korteUri}')
            continue
        print(f'{index+1}/{total_types} querying type {otl_asset_type.korteUri}')
        type_term.value = otl_asset_type.uuid
        query_dto.from_ = 0
        rows.extend([asset.uuid, otl_asset_type.korteUri, construct_naampad(asset),
                     f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}']
                    for asset in eminfra_client.search_assets(query_dto))
    table = PrettyTable(headers)
    table.add_rows(rows)

    with open('table.csv', 'w', newline='') as f_output:
        f_output.write(table.get_csv_string())
