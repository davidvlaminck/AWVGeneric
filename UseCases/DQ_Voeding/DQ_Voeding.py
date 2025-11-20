import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import RelatieEnum
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger, create_relatie_if_missing
from utils.query_dto_helpers import build_query_search_assettype
from utils.wkt_geometry_helpers import format_locatie_kenmerk_lgc_2_wkt

ASSETTYPE_UUID_KAST = '10377658-776f-4c21-a294-6c740b9f655e'
ASSETTYPE_UUID_LS = '80fdf1b4-e311-4270-92ba-6367d2a42d47'
ASSETTYPE_UUID_LSDEEL = 'b4361a72-e1d5-41c5-bfcc-d48f459f4048'
ASSETTYPE_UUID_HS = '46dcd9b1-f660-4c8c-8e3e-9cf794b4de75'
ASSETTYPE_UUID_HSDEEL = 'a9655f50-3de7-4c18-aa25-181c372486b1'


def add_relaties_vanuit_kast(client: EMInfraClient) -> None:
    """
    Toevoegen van alle relaties vanuit een Kast.
    Kast (Legacy): zoek de child-assets
    LS (Legacy): zoek assets via de relatie HeeftBijhorendeAsset

    Opzoeken van alle bron assets (Kast).
    Opzoeken van de doel assets (HS/HSDeel/LS/LSDeel).
    Toevoegen van de bevestiging- en voedings-relaties

    HeeftBijhorendeAssets vanuit LS.
    DNBLaagspanning en Energiemeter en ForfaitaireAansluiting.
    Voedings-relaties toevoegen en dubbele assets loggen.

    :param client:
    :return:
    """
    query_search_kast = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_KAST)
    generator_kast = client.search_assets(query_dto=query_search_kast, actief=True)
    for counter, asset in enumerate(generator_kast, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        locatie_kenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)

        child_assets_van_kast = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        assets_ls = [item for item in child_assets_van_kast if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        assets_lsdeel = [item for item in child_assets_van_kast if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']
        assets_hs = [item for item in child_assets_van_kast if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HS']
        assets_hsdeel = [item for item in child_assets_van_kast if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel']

        asset_ls = None
        if len(assets_ls) == 1:
            logging.debug("Bevestiging-relatie van Kast naar LS")
            asset_ls = assets_ls[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_ls,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_ls.uuid, doel_asset_uuid=asset.uuid)
        elif len(assets_ls) > 1:
            assets_meerdere_doelen.append(asset)

        asset_lsdeel = None
        if len(assets_lsdeel) == 1:
            logging.debug("Bevestiging-relatie van Kast naar LSDeel")
            asset_lsdeel = assets_lsdeel[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_lsdeel.uuid,
                                                              doel_asset_uuid=asset.uuid)
        elif len(assets_lsdeel) > 1:
            assets_meerdere_doelen.append(asset)

        if asset_ls and asset_lsdeel:
            logging.debug("Voedt-relatie van LS naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_ls, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)

        asset_hs = None
        if len(assets_hs) == 1:
            logging.debug("Bevestiging-relatie van Kast naar HS")
            asset_hs = assets_hs[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_hs,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_hs.uuid, doel_asset_uuid=asset.uuid)
        elif len(assets_hs) > 1:
            assets_meerdere_doelen.append(asset)

        asset_hsdeel = None
        if len(assets_hsdeel) == 1:
            logging.debug("Bevestiging-relatie van Kast naar HSDeel")
            asset_hsdeel = assets_hsdeel[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_hsdeel.uuid,
                                                              doel_asset_uuid=asset.uuid)
        elif len(assets_hsdeel) > 1:
            assets_meerdere_doelen.append(asset)

        if asset_hs and asset_hsdeel:
            logging.debug("Voedt-relatie van HS naar HSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_hs, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.VOEDT)
        if asset_hsdeel and asset_lsdeel:
            logging.debug("Voedt-relatie van HSDeel naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_hsdeel, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)

        heeftbijhorende_assets_van_ls = client.search_assets_via_relatie(asset_uuid=asset_ls.uuid,
                                                                         relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
        assets_dnblaagspanning = [item for item in heeftbijhorende_assets_van_ls if
                                  item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning']
        assets_energiemeterdnb = [item for item in heeftbijhorende_assets_van_ls if
                                  item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']
        assets_forfaitaireaansluiting = [item for item in heeftbijhorende_assets_van_ls if
                                         item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ForfaitaireAansluiting']

        asset_dnblaagspanning = None
        if len(assets_dnblaagspanning) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar DNBLaagspanning")
            asset_dnblaagspanning = assets_dnblaagspanning[0]
            create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning, doel_asset=asset_ls,
                                      relatie=RelatieEnum.HOORTBIJ)
            logging.debug("DNBLaagspanning heeft geen eigenschap locatie")
        elif len(assets_dnblaagspanning) > 1:
            assets_meerdere_doelen.append(asset_ls)

        asset_energiemeterdnb = None
        if len(assets_energiemeterdnb) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar EnergiemeterDNB")
            asset_energiemeterdnb = assets_energiemeterdnb[0]
            create_relatie_if_missing(client=client, bron_asset=asset_energiemeterdnb, doel_asset=asset_ls,
                                      relatie=RelatieEnum.HOORTBIJ)
            wkt_geometry = format_locatie_kenmerk_lgc_2_wkt(locatie=locatie_kenmerk)
            eminfra_client.update_geometrie_by_asset_uuid(asset_uuid=asset_energiemeterdnb.uuid,
                                                          wkt_geometry=wkt_geometry)
        elif len(assets_energiemeterdnb) > 1:
            assets_meerdere_doelen.append(asset_ls)

        asset_forfaitaireaansluiting = None
        if len(assets_forfaitaireaansluiting) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar ForfaitaireAansluiting")
            asset_forfaitaireaansluiting = assets_forfaitaireaansluiting[0]
            create_relatie_if_missing(client=client, bron_asset=asset_forfaitaireaansluiting, doel_asset=asset_ls,
                                      relatie=RelatieEnum.HOORTBIJ)
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid,
                                                                wkt_geom=locatie_kenmerk.geometrie)
        elif len(assets_forfaitaireaansluiting) > 1:
            assets_meerdere_doelen.append(asset_ls)

        if asset_dnblaagspanning and asset_energiemeterdnb:
            logging.debug("Voedt-relatie van DNBLaagspanning naar EnergiemeterDNB")
            create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning, doel_asset=asset_energiemeterdnb,
                                      relatie=RelatieEnum.VOEDT)
        if asset_dnblaagspanning and asset_forfaitaireaansluiting:
            logging.debug("Voedt-relatie van DNBLaagspanning naar ForfaitaireAansluiting")
            create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                      doel_asset=asset_forfaitaireaansluiting, relatie=RelatieEnum.VOEDT)

        if counter % 5 == 0:
            break


if __name__ == '__main__':
    configure_logger()
    logging.info('Kwaliteitscontrole van voeding-gerelateerde assets.')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    global assets_meerdere_doelen
    add_relaties_vanuit_kast(client=eminfra_client)
