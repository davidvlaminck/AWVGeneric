import logging

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, build_query_search_betrokkenerelaties, build_query_search_assets


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Vervang agent door diens opvolger.')
    logging.info('Hedwig Van Landeghem door Arne De Sterck')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    agent1 = next(eminfra_client.search_agent(naam="Hedwig Van Landeghem", actief=True), None)
    if not agent1:
        raise ValueError('Agent not found')
    agent2 = next(eminfra_client.search_agent(naam="Arne De Sterck", actief=True), None)
    if not agent2:
        raise ValueError('Agent not found')

    logging.info(f'Zoek alle assets met {agent1.naam} als toezichter.')
    query_assets = build_query_search_assets(agent=agent1)
    assets = eminfra_client.search_assets(query_dto=query_assets)

    for asset in iter(assets):
        logging.info(f'Zoek de betrokkenerelatie bij asset: {asset.uuid}.')
        query_betrokkenerelatie = build_query_search_betrokkenerelaties(bronAsset=asset, agent=agent1)
        if betrokkenerelatie := next(
            eminfra_client.search_betrokkenerelaties(
                query_dto=query_betrokkenerelatie
            ),
            None,
        ):
            logging.info(f'Wijzig de betrokkenerelatie: {betrokkenerelatie.uuid}'
                         f'\n\tbron (asset):\t{betrokkenerelatie.bron.get("uuid")}'
                         f'\n\tdoel (agent):\t{betrokkenerelatie.doel.get("uuid")}')
            logging.info(f'Vervang agent {agent1.naam} door {agent2.naam}. '
                         f'\nVoeg een nieuwe HeeftBetrokkene-relatie toe en schrap de huidige HeeftBetrokkene-relatie')
            try:
                eminfra_client.add_betrokkenerelatie(asset=asset, agent_uuid=agent2.uuid, rol=betrokkenerelatie.rol)
                eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid=betrokkenerelatie.uuid)
            except Exception as e:
                raise ValueError(
                    f'Exception occured: {e} updating HeeftBetrokkene-relatie.'
                    f'\nFailed to update agent from {agent1.naam} to {agent2.naam}'
                    f' with role: {betrokkenerelatie.rol}'
                ) from e

