import logging

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, build_query_search_betrokkenerelaties, build_query_search_assets


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Schrap agent "Arthur De Vos" bij OTL-assets')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    agent = next(eminfra_client.search_agent(naam="Arthur De Vos", actief=True), None)
    if not agent:
        raise ValueError('Agent not found')

    logging.info(f'Zoek alle assets met {agent.naam} als toezichter.')
    query_assets = build_query_search_assets(agent=agent)
    assets = eminfra_client.search_assets(query_dto=query_assets)

    for asset in iter(assets):
        logging.info(f'Zoek de betrokkenerelatie bij asset: {asset.uuid}.')
        query_betrokkenerelatie = build_query_search_betrokkenerelaties(bronAsset=asset, agent=agent)
        if betrokkenerelatie := next(
            eminfra_client.search_betrokkenerelaties(
                query_dto=query_betrokkenerelatie
            ),
            None,
        ):
            logging.info(f'Deactiveer betrokkenerelatie: {betrokkenerelatie.uuid} tussen'
                         f'\n\tbron (asset):\t{betrokkenerelatie.bron.get("uuid")}'
                         f'\n\tdoel (agent):\t{betrokkenerelatie.doel.get("uuid")}')
            eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid=betrokkenerelatie.uuid)