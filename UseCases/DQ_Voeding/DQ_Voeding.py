import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import RelatieEnum
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger, create_relatie_if_missing
from utils.query_dto_helpers import build_query_search_assettype
from utils.wkt_geometry_helpers import format_locatie_kenmerk_lgc_2_wkt

from otlmow_model.OtlmowModel.BaseClasses.OTLObject import dynamic_create_instance_from_uri

ASSETTYPE_UUID_KAST = '10377658-776f-4c21-a294-6c740b9f655e'
ASSETTYPE_UUID_LS = '80fdf1b4-e311-4270-92ba-6367d2a42d47'
ASSETTYPE_UUID_LSDEEL = 'b4361a72-e1d5-41c5-bfcc-d48f459f4048'
ASSETTYPE_UUID_HS = '46dcd9b1-f660-4c8c-8e3e-9cf794b4de75'
ASSETTYPE_UUID_HSDEEL = 'a9655f50-3de7-4c18-aa25-181c372486b1'
ASSETTYPE_UUID_HSCABINELEGACY = '1cf24e76-5bf3-44b0-8332-a47ab126b87e'

def add_relaties_vanuit_kast(client: EMInfraClient) -> [list]:
    """
    Toevoegen van de explicitiete relaties vanuit een Kast op basis van de naampaden.
    Kast (Legacy): zoek de child-assets
    LS (Legacy): zoek assets via het kenmerk HeeftBijhorendeAsset

    Opzoeken van alle bron assets (Kast).
    Opzoeken van de doel assets (LS/LSDeel).
    Toevoegen van de bevestiging- en voedings-relaties

    HeeftBijhorendeAssets vanuit LS.
    DNBLaagspanning en Energiemeter en ForfaitaireAansluiting.
    Voedings-relaties toevoegen en dubbele assets loggen.

    :param client:
    :return:
    """
    kast_meerdere_ls = []
    kast_meerdere_lsdeel = []
    ls_meerdere_dnb = []
    ls_meerdere_energiemeterdnb = []
    ls_meerdere_forfaitaireaansluiting = []

    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_KAST)
    generator_asset = client.search_assets(query_dto=search_query, actief=True)
    for counter, asset in enumerate(generator_asset, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        locatie_kenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        assets_ls = [item for item in child_assets if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        assets_lsdeel = [item for item in child_assets if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']

        asset_ls = None
        if len(assets_ls) == 1:
            logging.debug("Bevestiging-relatie van Kast naar LS")
            asset_ls = assets_ls[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_ls,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_ls.uuid, doel_asset_uuid=asset.uuid)
        elif len(assets_ls) > 1:
            kast_meerdere_ls.append(asset)

        asset_lsdeel = None
        if len(assets_lsdeel) == 1:
            logging.debug("Bevestiging-relatie van Kast naar LSDeel")
            asset_lsdeel = assets_lsdeel[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_lsdeel.uuid,
                                                              doel_asset_uuid=asset.uuid)
        elif len(assets_lsdeel) > 1:
            kast_meerdere_lsdeel.append(asset)

        if asset_ls and asset_lsdeel:
            logging.debug("Voedt-relatie van LS naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_ls, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)

        if not asset_ls:
            continue

        heeftbijhorende_assets = client.search_assets_via_relatie(asset_uuid=asset_ls.uuid,
                                                                  relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
        assets_dnblaagspanning = [item for item in heeftbijhorende_assets if
                                  item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning']
        assets_energiemeterdnb = [item for item in heeftbijhorende_assets if
                                  item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']
        assets_forfaitaireaansluiting = [item for item in heeftbijhorende_assets if
                                         item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ForfaitaireAansluiting']

        asset_dnblaagspanning = None
        if len(assets_dnblaagspanning) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar DNBLaagspanning")
            asset_dnblaagspanning = assets_dnblaagspanning[0]
            create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning, doel_asset=asset_ls,
                                      relatie=RelatieEnum.HOORTBIJ)
            logging.debug("DNBLaagspanning heeft geen eigenschap locatie")
        elif len(assets_dnblaagspanning) > 1:
            ls_meerdere_dnb.append(asset_ls)

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
            ls_meerdere_energiemeterdnb.append(asset_ls)

        asset_forfaitaireaansluiting = None
        if len(assets_forfaitaireaansluiting) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar ForfaitaireAansluiting")
            asset_forfaitaireaansluiting = assets_forfaitaireaansluiting[0]
            create_relatie_if_missing(client=client, bron_asset=asset_forfaitaireaansluiting, doel_asset=asset_ls,
                                      relatie=RelatieEnum.HOORTBIJ)
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid,
                                                                wkt_geom=locatie_kenmerk.geometrie)
        elif len(assets_forfaitaireaansluiting) > 1:
            ls_meerdere_forfaitaireaansluiting.append(asset_ls)

        if asset_dnblaagspanning and asset_energiemeterdnb:
            logging.debug("Voedt-relatie van DNBLaagspanning naar EnergiemeterDNB")
            create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                      doel_asset=asset_energiemeterdnb, relatie=RelatieEnum.VOEDT)
        if asset_dnblaagspanning and asset_forfaitaireaansluiting:
            logging.debug("Voedt-relatie van DNBLaagspanning naar ForfaitaireAansluiting")
            create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                      doel_asset=asset_forfaitaireaansluiting, relatie=RelatieEnum.VOEDT)

        if counter % 1 == 0:
            break

    return [kast_meerdere_ls, kast_meerdere_lsdeel,
            ls_meerdere_dnb, ls_meerdere_energiemeterdnb, ls_meerdere_forfaitaireaansluiting]

def add_relaties_vanuit_hscabine(client: EMInfraClient) -> [list]:
    """
    Toevoegen van de expliciete relaties vanuit een HSCabine (Legacy) op basis van de boomstructuur.
    HSCabine (Legacy): zoek de child-assets

    Opzoeken van alle bron assets (HSCabine (Legacy)).
    Opzoeken van de doel assets (HS/HSDeel/LSDeel).
    Toevoegen van de bevestiging- en voedings-relaties

    :param client:
    :return:
    """
    hscab_meerdere_hs = []
    hscab_meerdere_hsdeel = []
    hscab_meerdere_lsdeel = []
    hs_meerdere_dnbhoogspanning = []
    hs_meerdere_energiemeter= []

    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_HSCABINELEGACY)
    generator_asset = client.search_assets(query_dto=search_query, actief=True)
    for counter, asset in enumerate(generator_asset, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        locatie_kenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        assets_hs = [item for item in child_assets if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HS']
        assets_hsdeel = [item for item in child_assets if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel']
        assets_lsdeel = [item for item in child_assets if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']

        asset_hs = None
        if len(assets_hs) == 1:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar HS")
            asset_hs = assets_hs[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_hs,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_hs.uuid, doel_asset_uuid=asset.uuid)
        elif len(assets_hs) > 1:
            hscab_meerdere_hs.append(asset)

        asset_hsdeel = None
        if len(assets_hsdeel) == 1:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar HSDeel")
            asset_hsdeel = assets_hsdeel[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_hsdeel.uuid,
                                                              doel_asset_uuid=asset.uuid)
        elif len(assets_hsdeel) > 1:
            hscab_meerdere_hsdeel.append(asset)

        asset_lsdeel = None
        if len(assets_lsdeel) == 1:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar LSDeel")
            asset_lsdeel = assets_lsdeel[0]
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            eminfra_client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=asset_lsdeel.uuid,
                                                              doel_asset_uuid=asset.uuid)
        elif len(assets_lsdeel) > 1:
            hscab_meerdere_lsdeel.append(asset)

        if asset_hs and asset_hsdeel:
            logging.debug("Voedt-relatie van HS naar HSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_hs, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.VOEDT)

        if asset_hsdeel and asset_lsdeel:
            logging.debug("Voedt-relatie van HSDeel naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_hsdeel, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)

        if not asset_hs:
            continue

        heeftbijhorende_assets = client.search_assets_via_relatie(asset_uuid=asset_hs.uuid,
                                                                  relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
        assets_dnbhoogspanning = [item for item in heeftbijhorende_assets if
                                  item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning']
        assets_energiemeterdnb = [item for item in heeftbijhorende_assets if
                                  item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']

        asset_dnbhoogspanning = None
        if len(assets_dnbhoogspanning) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar DNBLaagspanning")
            asset_dnbhoogspanning = assets_dnbhoogspanning[0]
            create_relatie_if_missing(client=client, bron_asset=asset_dnbhoogspanning, doel_asset=asset_hs,
                                      relatie=RelatieEnum.HOORTBIJ)
            logging.debug("DNBLaagspanning heeft geen eigenschap locatie")
        elif len(assets_dnbhoogspanning) > 1:
            hs_meerdere_dnbhoogspanning.append(asset_hs)

        asset_energiemeterdnb = None
        if len(assets_energiemeterdnb) == 1:
            logging.debug("HeeftBijhorendeAssets-relatie van LS naar EnergiemeterDNB")
            asset_energiemeterdnb = assets_energiemeterdnb[0]
            create_relatie_if_missing(client=client, bron_asset=asset_energiemeterdnb, doel_asset=asset_hs,
                                      relatie=RelatieEnum.HOORTBIJ)
            wkt_geometry = format_locatie_kenmerk_lgc_2_wkt(locatie=locatie_kenmerk)
            eminfra_client.update_geometrie_by_asset_uuid(asset_uuid=asset_energiemeterdnb.uuid,
                                                          wkt_geometry=wkt_geometry)
        elif len(assets_energiemeterdnb) > 1:
            hs_meerdere_energiemeter.append(asset_hs)

        if counter % 1 == 0:
            break

    return [hscab_meerdere_hs, hscab_meerdere_hsdeel, hscab_meerdere_lsdeel, hs_meerdere_dnbhoogspanning,
            hs_meerdere_energiemeter]


if __name__ == '__main__':
    configure_logger()
    logging.info('Kwaliteitscontrole van voeding-gerelateerde assets.')
    eminfra_client = EMInfraClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=load_settings())

    [kast_meerdere_ls, kast_meerdere_lsdeel, ls_meerdere_dnb, ls_meerdere_energiemeterdnb,
     ls_meerdere_forfaitaireaansluiting] = add_relaties_vanuit_kast(client=eminfra_client)

    [hscab_meerdere_hs, hscab_meerdere_hsdeel, hscab_meerdere_lsdeel, hs_meerdere_dnbhoogspanning,
            hs_meerdere_energiemeter] = add_relaties_vanuit_hscabine(client=eminfra_client)

    # todo: bestaande en nieuwe geometrie vergelijken.
    # Pas niets aan indien de locatie reeds is afgeleid
    # Geef een warning als de afstand te groot is.

    # todo: logging toepassen voor de lijsten die worden teruggegeven uit de functies.
    logging.info("inspect data")

