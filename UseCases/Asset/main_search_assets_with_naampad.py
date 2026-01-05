import logging

from prettytable import PrettyTable

from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO, construct_naampad
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")

    cookie = "385f89b2ae054e20a7eb9339ed7ce578"
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
    logging.info(f'aantal OTL types: {total_types}')
    logging.debug('Enkel de assets met een naampad bevragen in plaats van alle assets omdat de parameter expansionsDTO'
                  ' field = parent wordt ingevuld.')

    logging.debug('value van installatie Astrid (LGC). '
                  'Wordt overschreven in onderstaande loop. Start met een willekeurige Legacy-asset.')
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
    headers = ['uuid', 'type', 'naampad', 'beheerobject_uuid', 'em_infra_link']
    rows = []
    for index, otl_asset_type in enumerate(asset_types):
        if otl_asset_type.uri in verweving_white_list:
            logging.info(f'{index+1}/{total_types} skipping type {otl_asset_type.korteUri}')
            continue
        logging.info(f'{index+1}/{total_types} querying type {otl_asset_type.korteUri}')
        type_term.value = otl_asset_type.uuid
        query_dto.from_ = 0
        rows.extend([asset.uuid, otl_asset_type.korteUri, construct_naampad(asset), asset.parent.uuid,
                     f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}']
                    for asset in eminfra_client.search_assets(query_dto) if construct_naampad(asset).startswith('DA-'))

    table = PrettyTable(headers)
    table.add_rows(rows)
    with open('table.csv', 'w', newline='') as f_output:
        f_output.write(table.get_csv_string())

    logging.info("Haal asset uit de boomstructuur")
    for row in rows:
        parent_uuid = row[3]
        asset_uuid = row[0]
        eminfra_client.remove_parent_from_asset(parent_uuid=parent_uuid, asset_uuid=asset_uuid)

    logging.info("Alle assets zijn uit de boomstructuur gehaald en het beheerobject bevat geen assets meer.")
    logging.info("Deactiveer het beheerobject.")
    beheerobjecten_uuids = {row[3] for row in rows}
    for beheerobject_uuid in beheerobjecten_uuids:
        beheerobject = eminfra_client.get_beheerobject_by_uuid(beheerobject_uuid=beheerobject_uuid)
        eminfra_client.update_beheerobject_status(beheerObject=beheerobject, status=False)