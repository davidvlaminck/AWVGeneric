import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import (RelatieEnum, OperatorEnum, PagingModeEnum, SelectionDTO, QueryDTO, ExpressionDTO,
                               TermDTO, LogicalOpEnum)
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger

ASSETTYPE_KAST = '10377658-776f-4c21-a294-6c740b9f655e'


def build_search_query(client: EMInfraClient, assettype_uuid: str, relatie_richting: str,
                       relatie: RelatieEnum) -> QueryDTO:
    if relatie_richting not in ('in', 'out'):
        raise ValueError('Parameter relatie_richting moet een van volgende waardes zijn: "in", "out".')
    if relatie not in (RelatieEnum.STURING, RelatieEnum.BEVESTIGING):
        raise ValueError('Parameter relatie moet een niet-gerichte relatie zijn. Ofwel Bevestiging ofwel Sturing.')
    _, relatietype_id = client.get_kenmerktype_and_relatietype_id(relatie= relatie)
    query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[
                                     TermDTO(property='actief',
                                             operator=OperatorEnum.EQ,
                                             value=True),
                                     TermDTO(property=f'rel:{relatie_richting}:{relatietype_id}',
                                                operator=OperatorEnum.EQ,
                                                value=None,
                                                logicalOp=LogicalOpEnum.AND),
                                     TermDTO(property='type',
                                                operator=OperatorEnum.EQ,
                                                value=assettype_uuid,
                                                logicalOp=LogicalOpEnum.AND)
                                        ])]))
    logging.debug(f'Inspect query_dto:\n{query_dto}')
    return query_dto

if __name__ == '__main__':
    configure_logger()
    logging.info('Omkeren van niet-gerichte assetsrelaties.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    search_query = build_search_query(client=eminfra_client, relatie_richting='in', relatie=RelatieEnum.BEVESTIGING,
                                      assettype_uuid=ASSETTYPE_KAST)
    generator = eminfra_client.search_assets(query_dto=search_query)

    counter_ls, counter_lsdeel, counter_vr = 0, 0, 0
    assettypes = []
    rows = []
    for idx, asset in enumerate(generator):
        logging.info(f"Processing asset: ({idx}): asset_uuid: {asset.uuid}")

        logging.info('Oplijsten van doel-assets')
        if assets_gelinkt := eminfra_client.search_assets_via_relatie(asset_uuid=asset.uuid, relatie=RelatieEnum.BEVESTIGING):
            logging.debug('Filter doelassets')
            assets_ls = [asset for asset in assets_gelinkt if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
            assets_lsdeel = [asset for asset in assets_gelinkt if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']
            assets_vrlegacy = [asset for asset in assets_gelinkt if asset.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#VRLegacy']

            if not assets_ls and not assets_lsdeel and not assets_vrlegacy:
                logging.debug("Bevestiging-relatie tussen Kast en ... ?")
                assettypes.extend(asset.type.uri for asset in assets_gelinkt)


            for asset_ls in assets_ls:
                counter_ls += 1
                logging.info('Verwijder de inkomende relatie vanuit LS.')
                eminfra_client.remove_assets_via_relatie(bronasset_uuid=asset.uuid, doelasset_uuid=asset_ls.uuid, relatie=RelatieEnum.BEVESTIGING)

                logging.info('Voeg een uitgaande relatie toe naar LS.')
                eminfra_client.create_assetrelatie(bronAsset=asset_ls, doelAsset=asset, relatie=RelatieEnum.BEVESTIGING)

            for asset_lsdeel in assets_lsdeel:
                counter_lsdeel += 1
                logging.info('Verwijder de inkomende relatie vanuit LSDeel.')
                eminfra_client.remove_assets_via_relatie(bronasset_uuid=asset.uuid, doelasset_uuid=asset_lsdeel.uuid, relatie=RelatieEnum.BEVESTIGING)

                logging.info('Voeg een uitgaande relatie toe naar LSDeel.')
                eminfra_client.create_assetrelatie(bronAsset=asset_lsdeel, doelAsset=asset, relatie=RelatieEnum.BEVESTIGING)

            for asset_vrlegacy in assets_vrlegacy:
                counter_vr += 1
                logging.info('Verwijder de inkomende relatie vanuit VR.')
                eminfra_client.remove_assets_via_relatie(bronasset_uuid=asset.uuid, doelasset_uuid=asset_vrlegacy.uuid, relatie=RelatieEnum.BEVESTIGING)

                logging.info('Voeg een uitgaande relatie toe naar VR.')
                eminfra_client.create_assetrelatie(bronAsset=asset_vrlegacy, doelAsset=asset, relatie=RelatieEnum.BEVESTIGING)
    logging.info(counter_ls)
    logging.info(counter_lsdeel)
    logging.info(counter_vr)

    logging.info(f'Overige assettypes: {assettypes}')