import logging
import pandas as pd

from API.eminfra.eminfra_client import EMInfraClient
from API.eminfra.eminfra_domain import RelatieEnum, AssetDTO
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
INSTALLATIE_TYPES = {
    "LS": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
    "LSDEEL": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel",
    "HS": "https://lgc.data.wegenenverkeer.be/ns/installatie#HS",
    "HSDEEL": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel",
}
ONDERDEEL_TYPES = {
    "DNB_LAAG": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning",
    "DNB_HOOG": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning",
    "EM_DNB": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB",
    "FORFAIT": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ForfaitaireAansluiting",
}

MAX_ITERATIONS = 10000000

def filter_assets(assets: [AssetDTO], type_uri: str) -> [AssetDTO]:
    """
    Filter from a list of assets the active assets that match an uri.
    """
    return [a for a in assets if a.type.uri == type_uri and a.actief]

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
        if wkt_geometry_child and wkt_geometry_parent:
            logging.debug('Update locatie indien de afstand tot de parent-asset groter is dan 0.0.')
            euclidean_distance = get_euclidean_distance_wkt(wkt_geometry_parent, wkt_geometry_child)
            if euclidean_distance > 0.0:
                logging.debug(f'Afstand tussen beide assets bedraagt: {euclidean_distance}')
                client.update_geometrie_by_asset_uuid(asset_uuid=child_asset.uuid, wkt_geometry=wkt_geometry_parent)
        elif not wkt_geometry_child and wkt_geometry_parent:
            logging.debug('update locatie van de child-asset.')
            client.update_geometrie_by_asset_uuid(asset_uuid=child_asset.uuid, wkt_geometry=wkt_geometry_parent)


def add_relaties_vanuit_kast(client: EMInfraClient) -> (list, list):
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
    asset_foute_relaties = []

    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_KAST)
    generator_asset = client.search_assets(query_dto=search_query, actief=True)
    for counter, asset in enumerate(generator_asset, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        if not child_assets:
            continue

        assets_ls = filter_assets(child_assets, INSTALLATIE_TYPES["LS"])
        assets_lsdeel = filter_assets(child_assets, INSTALLATIE_TYPES["LSDEEL"])

        if len(assets_lsdeel) > 1:
            asset_multiple_children.append(asset)

        if len(assets_ls) > 1:
            asset_multiple_children.append(asset)

        if len(assets_ls) == 1 and len(assets_lsdeel) == 1:
            logging.debug("Voedt-relatie van LS naar LSDeel")
            asset_ls = assets_ls[0]
            asset_lsdeel = assets_lsdeel[0]
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_ls, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)
            except Exception:
                asset_foute_relaties.append(asset_ls)

        for asset_lsdeel in assets_lsdeel:
            logging.debug("Bevestiging-relatie van Kast naar LSDeel")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_lsdeel, doel_asset=asset,
                                      relatie=RelatieEnum.BEVESTIGING)
                set_locatie(client=client, parent_asset=asset, child_asset=asset_lsdeel, set_afgeleide_locatie=True)
            except Exception:
                asset_foute_relaties.append(asset_lsdeel)
        for asset_ls in assets_ls:
            logging.debug("Bevestiging-relatie van Kast naar LS")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_ls, doel_asset=asset,
                                      relatie=RelatieEnum.BEVESTIGING)
                set_locatie(client=client, parent_asset=asset, child_asset=asset_ls, set_afgeleide_locatie=True)
            except Exception:
                asset_foute_relaties.append(asset_ls)

            logging.info('Boomstructuur vervolledigen voor alle LS.')
            heeftbijhorende_assets = client.search_assets_via_relatie(asset_uuid=asset_ls.uuid,
                                                                      relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
            if not heeftbijhorende_assets:
                continue

            assets_dnblaagspanning = filter_assets(heeftbijhorende_assets, ONDERDEEL_TYPES["DNB_LAAG"])
            assets_energiemeterdnb = filter_assets(heeftbijhorende_assets, ONDERDEEL_TYPES["EM_DNB"])
            assets_forfaitaireaansluiting = filter_assets(heeftbijhorende_assets, ONDERDEEL_TYPES["FORFAIT"])

            if len(assets_dnblaagspanning) > 1:
                asset_multiple_children.append(asset_ls)
            for asset_dnblaagspanning in assets_dnblaagspanning:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar DNBLaagspanning")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning, doel_asset=asset_ls,
                                          relatie=RelatieEnum.HOORTBIJ)
                except Exception:
                    asset_foute_relaties.append(asset_dnblaagspanning)

            if len(assets_energiemeterdnb) > 1:
                asset_multiple_children.append(asset_ls)
            for asset_energiemeterdnb in assets_energiemeterdnb:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar EnergiemeterDNB")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_energiemeterdnb, doel_asset=asset_ls,
                                          relatie=RelatieEnum.HOORTBIJ)
                    set_locatie(client=client, parent_asset=asset_ls, child_asset=asset_energiemeterdnb, set_afgeleide_locatie=False)
                except Exception:
                    asset_foute_relaties.append(asset_energiemeterdnb)

            if len(assets_forfaitaireaansluiting) > 1:
                asset_multiple_children.append(asset_ls)
            for asset_forfaitaireaansluiting in assets_forfaitaireaansluiting:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar ForfaitaireAansluiting")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_forfaitaireaansluiting, doel_asset=asset_ls,
                                          relatie=RelatieEnum.HOORTBIJ)
                    set_locatie(client=client, parent_asset=asset_ls, child_asset=asset_forfaitaireaansluiting,
                            set_afgeleide_locatie=False)
                except Exception:
                    asset_foute_relaties.append(asset_forfaitaireaansluiting)


            if len(assets_dnblaagspanning) == 1 and len(assets_energiemeterdnb) == 1:
                asset_dnblaagspanning = assets_dnblaagspanning[0]
                asset_energiemeterdnb = assets_energiemeterdnb[0]
                logging.debug("Voedt-relatie van DNBLaagspanning naar EnergiemeterDNB")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                          doel_asset=asset_energiemeterdnb, relatie=RelatieEnum.VOEDT)
                except Exception:
                    asset_foute_relaties.append(asset_dnblaagspanning)
            if len(assets_dnblaagspanning) == 1 and len(assets_forfaitaireaansluiting) == 1:
                asset_dnblaagspanning = assets_dnblaagspanning[0]
                asset_forfaitaireaansluiting = assets_forfaitaireaansluiting[0]
                logging.debug("Voedt-relatie van DNBLaagspanning naar ForfaitaireAansluiting")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_dnblaagspanning,
                                          doel_asset=asset_forfaitaireaansluiting, relatie=RelatieEnum.VOEDT)
                except Exception:
                    asset_foute_relaties.append(asset_dnblaagspanning)

        if counter % MAX_ITERATIONS == 0:
            break

    return asset_multiple_children, asset_foute_relaties

def add_relaties_vanuit_hscabine(client: EMInfraClient) -> (list, list):
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
    asset_foute_relaties = []

    search_query = build_query_search_assettype(assettype_uuid=ASSETTYPE_UUID_HSCABINELEGACY)
    generator_asset = client.search_assets(query_dto=search_query, actief=True)
    for counter, asset in enumerate(generator_asset, start=1):
        logging.info(f'Processing ({counter}) asset: {asset.uuid}')

        child_assets = list(client.search_child_assets(asset_uuid=asset.uuid, recursive=False))
        if not child_assets:
            continue

        assets_hsdeel = filter_assets(child_assets, INSTALLATIE_TYPES["HSDEEL"])
        assets_lsdeel = filter_assets(child_assets, INSTALLATIE_TYPES["LSDEEL"])
        assets_hs = filter_assets(child_assets, INSTALLATIE_TYPES["HS"])

        if len(assets_hsdeel) > 1:
            asset_multiple_children.append(asset)
        for asset_hsdeel in assets_hsdeel:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar HSDeel")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_hsdeel, doel_asset=asset,
                                      relatie=RelatieEnum.BEVESTIGING)
                set_locatie(client=client, parent_asset=asset, child_asset=asset_hsdeel)
            except Exception:
                asset_foute_relaties.append(asset_hsdeel)

        if len(assets_lsdeel) > 1:
            asset_multiple_children.append(asset)
        for asset_lsdeel in assets_lsdeel:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar LSDeel")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_lsdeel, doel_asset=asset,
                                      relatie=RelatieEnum.BEVESTIGING)
                set_locatie(client=client, parent_asset=asset, child_asset=asset_lsdeel)
            except Exception:
                asset_foute_relaties.append(asset_lsdeel)

        if len(assets_hs) == 1 and len(assets_hsdeel) == 1:
            asset_hs = assets_hs[0]
            asset_hsdeel = assets_hsdeel[0]
            logging.debug("Voedt-relatie van HS naar HSDeel")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_hs, doel_asset=asset_hsdeel,
                                      relatie=RelatieEnum.VOEDT)
            except Exception:
                asset_foute_relaties.append(asset_hs)

        if len(assets_hsdeel) == 1 and len(assets_lsdeel) == 1:
            asset_hsdeel = assets_hsdeel[0]
            asset_lsdeel = assets_lsdeel[0]
            logging.debug("Voedt-relatie van HSDeel naar LSDeel")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_hsdeel, doel_asset=asset_lsdeel,
                                      relatie=RelatieEnum.VOEDT)
            except Exception:
                asset_foute_relaties.append(asset_hsdeel)

        if len(assets_hs) > 1:
            asset_multiple_children.append(asset)
        for asset_hs in assets_hs:
            logging.debug("Bevestiging-relatie van HSCabineLegacy naar HS")
            try:
                create_relatie_if_missing(client=client, bron_asset=asset_hs, doel_asset=asset,
                                      relatie=RelatieEnum.BEVESTIGING)
                set_locatie(client=client, parent_asset=asset, child_asset=asset_hs)
            except Exception:
                asset_foute_relaties.append(asset_hs)

            heeftbijhorende_assets = client.search_assets_via_relatie(asset_uuid=asset_hs.uuid,
                                                                      relatie=RelatieEnum.HEEFTBIJHORENDEASSETS)
            if not heeftbijhorende_assets:
                continue

            assets_dnbhoogspanning = filter_assets(heeftbijhorende_assets, ONDERDEEL_TYPES["DNB_HOOG"])
            assets_energiemeterdnb = filter_assets(heeftbijhorende_assets, ONDERDEEL_TYPES["EM_DNB"])

            if len(assets_dnbhoogspanning) > 1:
                asset_multiple_children.append(asset_hs)
            for asset_dnbhoogspanning in assets_dnbhoogspanning:
                logging.debug("HeeftBijhorendeAssets-relatie van HS naar DNBHoogspanning")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_dnbhoogspanning, doel_asset=asset_hs,
                                          relatie=RelatieEnum.HOORTBIJ)
                    logging.debug("DNBLaagspanning heeft geen eigenschap locatie")
                except Exception:
                    asset_foute_relaties.append(asset_dnbhoogspanning)

            if len(assets_energiemeterdnb) > 1:
                asset_multiple_children.append(asset_hs)
            for asset_energiemeterdnb in assets_energiemeterdnb:
                logging.debug("HeeftBijhorendeAssets-relatie van LS naar EnergiemeterDNB")
                try:
                    create_relatie_if_missing(client=client, bron_asset=asset_energiemeterdnb, doel_asset=asset_hs,
                                          relatie=RelatieEnum.HOORTBIJ)
                    set_locatie(client=client, parent_asset=asset_hs, child_asset=asset_energiemeterdnb, set_afgeleide_locatie=False)
                except Exception:
                    asset_foute_relaties.append(asset_energiemeterdnb)

        if counter % MAX_ITERATIONS == 0:
            break

    return asset_multiple_children, asset_foute_relaties


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
    environment = Environment.PRD
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=load_settings())

    asset_multiple_children_kast, asset_foute_relaties_kast = add_relaties_vanuit_kast(client=eminfra_client)
    asset_multiple_children_hscabine, asset_foute_relaties_hscabine = add_relaties_vanuit_hscabine(client=eminfra_client)

    asset_multiple_children_kast = [format_asset_to_dict(asset=a) for a in asset_multiple_children_kast]
    asset_multiple_children_hscabine = [format_asset_to_dict(asset=a) for a in asset_multiple_children_hscabine]
    asset_foute_relaties_kast = [format_asset_to_dict(asset=a) for a in asset_foute_relaties_kast]
    asset_foute_relaties_hscabine = [format_asset_to_dict(asset=a) for a in asset_foute_relaties_hscabine]

    output_excel_path = f'DQ Voeding assets met meerdere child assets_{environment.value[0]}.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df1 = pd.DataFrame(asset_multiple_children_kast)
        df1.to_excel(writer, sheet_name='Kast', index=False, freeze_panes=[1, 1])
        df2 = pd.DataFrame(asset_multiple_children_hscabine)
        df2.to_excel(writer, sheet_name='HSCabine', index=False, freeze_panes=[1, 1])

    output_excel_path = f'DQ Voeding assets met foute relaties_{environment.value[0]}.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df1 = pd.DataFrame(asset_foute_relaties_kast)
        df1.to_excel(writer, sheet_name='Kast', index=False, freeze_panes=[1, 1])
        df2 = pd.DataFrame(asset_foute_relaties_hscabine)
        df2.to_excel(writer, sheet_name='HSCabine', index=False, freeze_panes=[1, 1])