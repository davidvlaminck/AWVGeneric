import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import RelatieEnum
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger, create_relatie_if_missing
from utils.query_dto_helpers import build_query_search_assettype

ASSETTYPE_UUID_KAST = '10377658-776f-4c21-a294-6c740b9f655e'

def add_relaties_kast(client: EMInfraClient):
    """
    Opzoeken van alle kasten.
    Opzoeken van de child assets per Kast en opdelen in LS en LSDeel.
    Toevoegen van een Bevestiging-relatie van de Kast naar LS en LSDeel.

    :param client:
    :return:
    """
    query_search_kast = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_KAST)
    generator_kast = client.search_assets(query_dto=query_search_kast, actief=True)
    for counter, asset in enumerate(generator_kast, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        ls_assets = [item for item in child_assets if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        ls_deel_assets = [item for item in child_assets if
                          item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']

        if len(ls_assets) == 1:
            ls = ls_assets[0]
            client.create_assetrelatie(bronAsset=asset, doelAsset=ls, relatie=RelatieEnum.BEVESTIGING)
            # todo plaats bovenstaande in de generieke functie
            create_relatie_if_missing()
        if len(ls_deel_assets) == 1:
            lsdeel = ls_assets[0]
            client.create_assetrelatie(bronAsset=asset, doelAsset=lsdeel, relatie=RelatieEnum.BEVESTIGING)
            # todo plaats bovenstaande in de generieke functie
            create_relatie_if_missing()

if __name__ == '__main__':
    configure_logger()
    logging.info('Kwaliteitscontrole van voeding-gerelateerde assets.')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    add_relaties_kast(client=eminfra_client)
