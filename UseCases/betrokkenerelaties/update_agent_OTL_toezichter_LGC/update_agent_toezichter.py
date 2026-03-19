import json
import logging

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import AssetDTO

from UseCases.utils import load_settings_path, configure_logger, build_query_search_by_naampad, \
    build_query_search_betrokkenerelaties

ENVIRONMENT = Environment.TEI


def splits_assets(assets: list[AssetDTO]) -> tuple[list[AssetDTO], list[AssetDTO]]:
    """
    Splits a list of assets into Legacy- and OTL-assets

    param assets: list of assets
    type assets: list[AssetDTO]
    rtype: tuple[list[AssetDTO], list[AssetDTO]]
    """
    lgc_assets = []
    otl_assets = []
    for a in assets:
        if a.type.uri.startswith('https://lgc.data.wegenenverkeer.be'):
            lgc_assets.append(a)
        elif a.type.uri.startswith('https://wegenenverkeer.data.vlaanderen.be/'):
            otl_assets.append(a)
        else:
            raise NotImplementedError(a.type.uri)
    return lgc_assets, otl_assets



if __name__ == '__main__':
    configure_logger()
    logging.info('Patrick Van Ransbeeck')
    logging.info('Voor alle assets in een bepaalde boomstructuur')
    logging.info('Update toezichter (Legacy-assets)')
    logging.info('Update toezichtsgroep (Legacy-assets)')
    logging.info('Update agent (OTL-assets). Rollen: toezichter, schadebeheerder, toezichtsgroep')

    # Open the JSON file and load its contents
    with open('agent_toezichter_config.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    beheerobjecten = data.keys()

    eminfra_client = EMInfraClient(env=ENVIRONMENT, auth_type=AuthType.JWT, settings_path=load_settings_path())

    for beheerobject in beheerobjecten:
        toezichter_info_voorgaand = data[beheerobject]['voorgaand']
        toezichter_info_nieuw = data[beheerobject]['nieuw']

        toezichter_gebruikersnaam_voorgaand = toezichter_info_voorgaand['toezichter_gebruikersnaam']
        toezichter_uuid_voorgaand = toezichter_info_voorgaand['toezichter_uuid']
        agent_uuid_voorgaand = toezichter_info_voorgaand['agent_uuid']
        agent_naam_voorgaand = toezichter_info_voorgaand['agent_naam']

        toezichter_naam = toezichter_info_nieuw['toezichter_naam']
        toezichter_uuid = toezichter_info_nieuw['toezichter_uuid']
        toezichtgroep_lgc_uuid = toezichter_info_nieuw['toezichtgroep_lgc_uuid']
        toezichtgroep_otl_uuid = toezichter_info_nieuw['toezichtgroep_otl_uuid']
        schadebeheerder_lgc_uuid = toezichter_info_nieuw['schadebeheerder_lgc_uuid']
        schadebeheerder_lgc_naam = toezichter_info_nieuw['schadebeheerder_lgc_naam']
        schadebeheerder_otl_uuid = toezichter_info_nieuw['schadebeheerder_otl_uuid']
        agent_uuid = toezichter_info_nieuw['agent_uuid']
        agent_naam = toezichter_info_nieuw['agent_naam']

        logging.info(f'Search all assets starting with a given beheerobject: {beheerobject}')
        query_dto = build_query_search_by_naampad(naampad=beheerobject)
        assets = eminfra_client.asset_service.search_assets_generator(query_dto=query_dto, actief=True)
        assets_list = list(assets)

        logging.info('Splits assets in LGC- en OTL-assets')
        lgc_assets, otl_assets = splits_assets(assets=assets_list)

        logging.info('LGC-assets: update Toezichter, Toezichtsgroep')
        for idx, asset in enumerate(lgc_assets):
            logging.info('Filter assets waarbij de voorgaande (huidige) toezichter overeenstemt met de config-file.')
            toezichterkenmerk_voorgaand = eminfra_client.toezichter_service.get_toezichter_by_uuid(asset_uuid=asset.uuid)
            if toezichterkenmerk_voorgaand.toezichter is not None and toezichterkenmerk_voorgaand.toezichter.uuid == toezichter_uuid_voorgaand:
                logging.info(f'Update asset {idx}: {asset.uuid}.')
                logging.info(f'Wijzig toezichter voorgaand "{toezichter_gebruikersnaam_voorgaand}" naar "{toezichter_naam}".')
                logging.info('Wijzig toezichtsgroep naar TOV.')
                # uuid van toezichter ophalen, omdat deze verschilt per omgeving (TEI/PRD)
                toezichter = list(eminfra_client.toezichter_service.search_identiteit(naam=toezichter_naam))[0]
                eminfra_client.toezichter_service.add_toezichter(asset_uuid=asset.uuid
                                                                 , toezichter_uuid=toezichter.uuid
                                                                 , toezichtgroep_uuid=toezichtgroep_lgc_uuid)
                logging.info('Wijzig schadebeheerder naar District Antwerpen of District Leuven.')
                schadebeheerder = eminfra_client.schadebeheerder_service.get_schadebeheerder_by_name(name=schadebeheerder_lgc_naam)
                eminfra_client.schadebeheerder_service.add_schadebeheerder_by_uuid(asset_uuid=asset.uuid, schadebeheerder=schadebeheerder[0])


        logging.info('OTL-assets: update Agent, rollen: toezichter, toezichtsgroep, schadebeheerder')

        agent_list = list(eminfra_client.agent_service.search_agent(naam=agent_naam))
        if len(agent_list) == 0:
            log_message = f'Geen agent teruggevonden voor: {agent_naam}'
            logging.warning(log_message)
            # raise ValueError(log_message)
        elif len(agent_list) != 1:
            log_message = f'Meerdere agenten teruggevonden voor: {agent_naam}'
            logging.warning(log_message)
            # raise ValueError(log_message)
        else:
            agent = agent_list[0]

        for idx, asset in enumerate(otl_assets):
            logging.info(f'Update asset {idx}: {asset.uuid}.')

            logging.info('Filter assets waarbij de voorgaande (huidige) toezichter (agent) (OTL) van een asset overeenstemt met de config-file.')
            query_dto = build_query_search_betrokkenerelaties(bron_asset=asset, rol='toezichter')
            betrokkenerelaties_list = list(eminfra_client.toezichter_service.search_betrokkenerelaties(query_dto=query_dto))
            if len(betrokkenerelaties_list) == 0:
                log_message = 'Geen betrokkenerelaties toezichter gevonden.'
                logging.warning(log_message)
            elif len(betrokkenerelaties_list) != 1:
                log_message = 'Meerdere betrokkenerelaties toezichter gevonden.'
            else:
                toezichter_voorgaand = betrokkenerelaties_list[0]

                if toezichter_voorgaand.uuid == agent_uuid_voorgaand:
                    eminfra_client.toezichter_service.add_betrokkenerelatie(asset=asset, agent_uuid=agent.uuid, rol='toezichter')
                    eminfra_client.toezichter_service.add_betrokkenerelatie(asset=asset, agent_uuid=toezichtgroep_otl_uuid, rol='toezichtsgroep')
                    eminfra_client.toezichter_service.add_betrokkenerelatie(asset=asset, agent_uuid=schadebeheerder_otl_uuid, rol='schadebeheerder')