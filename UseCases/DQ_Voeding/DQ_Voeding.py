import logging
import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import RelatieEnum, AssetDTO
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger, create_relatie_if_missing
from utils.query_dto_helpers import build_query_search_assettype
from utils.wkt_geometry_helpers import format_locatie_kenmerk_lgc_2_wkt, get_euclidean_distance_wkt

ASSETTYPE_UUID_KAST = '10377658-776f-4c21-a294-6c740b9f655e'
ASSETTYPE_UUID_LS = '80fdf1b4-e311-4270-92ba-6367d2a42d47'
ASSETTYPE_UUID_LSDEEL = 'b4361a72-e1d5-41c5-bfcc-d48f459f4048'
ASSETTYPE_UUID_HS = '46dcd9b1-f660-4c8c-8e3e-9cf794b4de75'
ASSETTYPE_UUID_HSDEEL = 'a9655f50-3de7-4c18-aa25-181c372486b1'
ASSETTYPE_UUID_HSCABINELEGACY = '1cf24e76-5bf3-44b0-8332-a47ab126b87e'


def set_locatie(client: EMInfraClient, parent_asset: AssetDTO, child_asset: AssetDTO, set_afgeleide_locatie: bool = True) -> None:
    """
    Update locatie kenmerk via een afgeleide of een absolute locatie.
    Instellen van de locatie voor de child asset op basis van de locatie van de parent asset.
    """
    if set_afgeleide_locatie:
        client.update_kenmerk_locatie_via_relatie(bron_asset_uuid=child_asset.uuid,
                                                          doel_asset_uuid=parent_asset.uuid)
    else:
        locatie_kenmerk_parent = client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=parent_asset.uuid)
        locatie_kenmerk_child = client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=child_asset.uuid)
        wkt_geometry_parent = format_locatie_kenmerk_lgc_2_wkt(locatie_kenmerk_parent)
        wkt_geometry_child = format_locatie_kenmerk_lgc_2_wkt(locatie_kenmerk_child)
        if wkt_geometry_parent and wkt_geometry_child:
            if get_euclidean_distance_wkt(wkt_geometry_parent, wkt_geometry_child) > 0.0:
                logging.debug('Update locatie indien de afstand tot de parent-asset groter is dan 0.0.')
                client.update_geometrie_by_asset_uuid(asset_uuid=child_asset.uuid, wkt_geometry=wkt_geometry_parent)
        elif wkt_geometry_child is None and wkt_geometry_parent is not None:
            logging.debug('update locatie van de child-asset.')
            client.update_geometrie_by_asset_uuid(asset_uuid=child_asset.uuid, wkt_geometry=wkt_geometry_parent)


def add_relaties_vanuit_kast(client: EMInfraClient) -> list:
    """
    Toevoegen van de explicitiete relaties vanuit een Kast op basis van de naampaden.
    Kast (Legacy): zoek de child-assets
    LS (Legacy): zoek assets via het kenmerk HeeftBijhorendeAsset

    Opzoeken van alle bron assets (Kast).
    Opzoeken van de doel assets (LS/LSDeel).
    Toevoegen van de bevestiging- en voedings-relaties

    HeeftBijhorendeAssets vanuit LS.
    DNBLaagspanning en Energiemeter en ForfaitaireAansluiting.
    Voedings-relaties toevoegen.

    :param client:
    :return:
    Assets teruggeven die meerdere child-assets bevatten, terwijl slechts 1 child-asset wordt verwacht.
    """
    asset_multiple_children = []

    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_KAST)
    generator_asset = client.search_assets(query_dto=search_query, actief=True)
    for counter, asset in enumerate(generator_asset, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        assets_ls = [item for item in child_assets if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS']
        assets_lsdeel = [item for item in child_assets if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']

        if len(assets_lsdeel) > 1:
            asset_multiple_children.append(asset)

        if len(assets_ls) > 1:
            asset_multiple_children.append(asset)

        if len(assets_ls) == 1 and len(assets_lsdeel) == 1:
            logging.debug("Voedt-relatie van LS naar LSDeel")
            asset_ls = assets_ls[0]
            asset_lsdeel = assets_lsdeel[0]
            create_relatie_if_missing(client=client, bron_asset=asset_ls, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)

        for asset_lsdeel in assets_lsdeel:
            logging.debug("Bevestiging-relatie van Kast naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            set_locatie(client=client, parent_asset=asset, child_asset=asset_lsdeel, set_afgeleide_locatie=True)
        for asset_ls in assets_ls:
            logging.debug("Bevestiging-relatie van Kast naar LS")
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_ls,
                                      relatie=RelatieEnum.BEVESTIGING)
            set_locatie(client=client, parent_asset=asset, child_asset=asset_ls, set_afgeleide_locatie=True)

            logging.info('Boomstructuur vervolledigen voor alle LS.')
            heeftbijhorende_assets = client.search_assets_via_relatie(asset_uuid=asset_ls.uuid,
                                                                      relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
            assets_dnblaagspanning = [item for item in heeftbijhorende_assets if
                                      item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning']
            assets_energiemeterdnb = [item for item in heeftbijhorende_assets if
                                      item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']
            assets_forfaitaireaansluiting = [item for item in heeftbijhorende_assets if
                                             item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ForfaitaireAansluiting']


            if len(assets_dnblaagspanning) > 1:
                asset_multiple_children.append(asset_ls)
            for asset_dnblaagspanning in assets_dnblaagspanning:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar DNBLaagspanning")
                create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning, doel_asset=asset_ls,
                                          relatie=RelatieEnum.HOORTBIJ)

            if len(assets_energiemeterdnb) > 1:
                asset_multiple_children.append(asset_ls)
            for asset_energiemeterdnb in assets_energiemeterdnb:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar EnergiemeterDNB")
                create_relatie_if_missing(client=client, bron_asset=asset_energiemeterdnb, doel_asset=asset_ls,
                                          relatie=RelatieEnum.HOORTBIJ)
                set_locatie(client=client, parent_asset=asset_ls, child_asset=asset_energiemeterdnb, set_afgeleide_locatie=False)

            if len(assets_forfaitaireaansluiting) > 1:
                asset_multiple_children.append(asset_ls)
            for asset_forfaitaireaansluiting in assets_forfaitaireaansluiting:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar ForfaitaireAansluiting")
                create_relatie_if_missing(client=client, bron_asset=asset_forfaitaireaansluiting, doel_asset=asset_ls,
                                          relatie=RelatieEnum.HOORTBIJ)
                set_locatie(client=client, parent_asset=asset_ls, child_asset=asset_forfaitaireaansluiting,
                            set_afgeleide_locatie=False)

            if len(assets_dnblaagspanning) == 1 and len(assets_energiemeterdnb) == 1:
                asset_dnblaagspanning = assets_dnblaagspanning[0]
                asset_energiemeterdnb = assets_energiemeterdnb[0]
                logging.debug("Voedt-relatie van DNBLaagspanning naar EnergiemeterDNB")
                create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                          doel_asset=asset_energiemeterdnb, relatie=RelatieEnum.VOEDT)
            if len(assets_dnblaagspanning) == 1 and len(assets_forfaitaireaansluiting) == 1:
                asset_dnblaagspanning = assets_dnblaagspanning[0]
                asset_forfaitaireaansluiting = assets_forfaitaireaansluiting[0]
                logging.debug("Voedt-relatie van DNBLaagspanning naar ForfaitaireAansluiting")
                create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                          doel_asset=asset_forfaitaireaansluiting, relatie=RelatieEnum.VOEDT)

        if counter % 10 == 0:
            break

    return asset_multiple_children

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
    asset_multiple_children = []

    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_HSCABINELEGACY)
    generator_asset = client.search_assets(query_dto=search_query, actief=True)
    for counter, asset in enumerate(generator_asset, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        assets_hsdeel = [item for item in child_assets if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel']
        assets_lsdeel = [item for item in child_assets if
                         item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']
        assets_hs = [item for item in child_assets if
                     item.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HS']

        if len(assets_hsdeel) > 1:
            asset_multiple_children.append(asset)
        for asset_hsdeel in assets_hsdeel:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar HSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            set_locatie(client=eminfra_client, parent_asset=asset, child_asset=asset_hsdeel)

        if len(assets_lsdeel) > 1:
            asset_multiple_children.append(asset)
        for asset_lsdeel in assets_lsdeel:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.BEVESTIGING)
            set_locatie(client=eminfra_client, parent_asset=asset, child_asset=asset_lsdeel)

        if len(assets_hs) == 1 and len(assets_hsdeel) == 1:
            asset_hs = assets_hs[0]
            asset_hsdeel = assets_hsdeel[0]
            logging.debug("Voedt-relatie van HS naar HSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_hs, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.VOEDT)

        if len(assets_hsdeel) == 1 and len(assets_lsdeel) == 1:
            asset_hsdeel = assets_hsdeel[0]
            asset_lsdeel = assets_lsdeel[0]
            logging.debug("Voedt-relatie van HSDeel naar LSDeel")
            create_relatie_if_missing(client=client, bron_asset=asset_hsdeel, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)

        if len(assets_hs) > 1:
            asset_multiple_children.append(asset)
        for asset_hs in assets_hs:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar HS")
            create_relatie_if_missing(client=client, bron_asset=asset_hs, doel_asset=asset,
                                      relatie=RelatieEnum.BEVESTIGING)
            set_locatie(client=eminfra_client, parent_asset=asset, child_asset=asset_hs)

            heeftbijhorende_assets = client.search_assets_via_relatie(asset_uuid=asset_hs.uuid,
                                                                      relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
            assets_dnbhoogspanning = [item for item in heeftbijhorende_assets if
                                      item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning']
            assets_energiemeterdnb = [item for item in heeftbijhorende_assets if
                                      item.type.uri == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB']

            if len(assets_dnbhoogspanning) > 1:
                asset_multiple_children.append(asset_hs)
            for asset_dnbhoogspanning in assets_dnbhoogspanning:
                logging.debug("HeeftBijhorendeAssets-relatie van HS naar DNBHoogspanning")
                create_relatie_if_missing(client=client, bron_asset=asset_dnbhoogspanning, doel_asset=asset_hs,
                                          relatie=RelatieEnum.HOORTBIJ)
                logging.debug("DNBLaagspanning heeft geen eigenschap locatie")

            if len(assets_energiemeterdnb) > 1:
                asset_multiple_children.append(asset_hs)
            for asset_energiemeterdnb in assets_energiemeterdnb:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar EnergiemeterDNB")
                create_relatie_if_missing(client=client, bron_asset=asset_energiemeterdnb, doel_asset=asset_hs,
                                          relatie=RelatieEnum.HOORTBIJ)
                set_locatie(client=eminfra_client, parent_asset=asset_hs, child_asset=asset_energiemeterdnb, set_afgeleide_locatie=False)

        if counter % 10 == 0:
            break

    return asset_multiple_children


def format_asset_to_dict(asset: AssetDTO) -> dict:
    """
    Format an AssetDTO objects to a dictionary. The dicitonary is used to write data to a pandas dataframe.
    Returns a dictionary.
    """
    return {
        "uuid": asset.uuid,
        "type": asset.type.uri,
        "naam": asset.naam,
        "actief": asset.actief,
        "commentaar": asset.commentaar
    }


if __name__ == '__main__':
    configure_logger()
    logging.info('Kwaliteitscontrole van voeding-gerelateerde assets.')
    eminfra_client = EMInfraClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=load_settings())

    asset_multiple_children_kast = add_relaties_vanuit_kast(client=eminfra_client)

    asset_multiple_children_hscabine = add_relaties_vanuit_hscabine(client=eminfra_client)

    asset_multiple_children_kast = [format_asset_to_dict(asset=a) for a in asset_multiple_children_kast]
    asset_multiple_children_hscabine = [format_asset_to_dict(asset=a) for a in asset_multiple_children_hscabine]
    output_excel_path = 'DQ Voeding assets met meerdere child assets.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(asset_multiple_children_kast)
        df.to_excel(writer, sheet_name='Kast', index=False, freeze_panes=[1, 1])
        df = pd.DataFrame(asset_multiple_children_hscabine)
        df.to_excel(writer, sheet_name='HSCabine', index=False, freeze_panes=[1, 1])