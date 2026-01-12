import logging
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import RelatieEnum, AssetDTO, AssettypeDTO
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger, create_relatie_if_missing, read_rsa_report

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


def filter_assets(assets: list[AssetDTO], assettype: AssettypeDTO) -> [AssetDTO]:
    """
    Filter from a list of assets the active assets that match an uri.
    """
    return [
        a for a in assets
        if a.actief
        and a.type.uri == assettype.uri]


def process_relatie_locatie(client: EMInfraClient, df: pd.DataFrame, assettype: AssettypeDTO,
                            relatie: RelatieEnum, doel_asset_is_parent: bool = True,
                            set_afgeleide_locatie: bool = True) -> None:
    """
    Process a dataframe with assets.

    Leg een relatie naar een specifiek assettype dat aanwezig is in de boomstructuur van die asset.
    Optioneel: leid de locatie af via de relatie.

    Voor iedere asset uit het dataframe worden alle assets uit die boomstructuur opgelijst.
    Vervolgens wordt gezocht naar het enige, unieke assettype uit die boomstructuur.
    Indien de relatie nog onbestaand is, wordt deze gelegd.
    De relatie wordt gezocht met de rechtstreekse parent (doel_asset_is_parent=True),
     of met elke asset uit de boomstructuur (doel_asset_is_parent=False).
    Zodra de relatie ligt, wordt de locatie afgeleid op basis van de locatie.

    Voorbeeld: LSDeel is Bevestigd aan een Kast. Vervolgens wordt de locatie afgeleid via de Bevestiging-relatie.

    :param client:
    :type client:
    :param df: Dataframe
    :type df: pd.DataFrame
    :param assettype: assettype
    :type assettype: AssettypeDTO
    :param relatie: Relatie
    :type relatie: RelatieEnum
    :param doel_asset_is_parent: De doel asset waarnaar de relatie wordt gelegd is de rechtstreekse parent van de asset
    :type doel_asset_is_parent: bool
    :param set_afgeleide_locatie: Leid de locatie af via de relatie
    :type set_afgeleide_locatie: bool
    :return: None
    """
    for _, df_row in df.iterrows():
        asset_uuid = df_row["uuid"]
        asset = client.asset_service.get_asset_by_uuid(asset_uuid=asset_uuid)
        logging.debug(f"Processing asset. assettype: {asset.type.korteUri}; uuid: {asset.uuid};"
                      f" relatie: {relatie.value}; set_afgeleide_locatie: {set_afgeleide_locatie}")
        if doel_asset_is_parent:
            parent_asset = client.asset_service.search_parent_asset(asset=asset, recursive=False, return_all_parents=False)
            doel_assets = [parent_asset]
        else:
            parent_asset = client.asset_service.search_parent_asset(asset=asset, recursive=True, return_all_parents=False)
            doel_assets = list(client.asset_service.search_child_assets_generator(asset=parent_asset, recursive=True))
        if all(type(a) == AssetDTO for a in doel_assets):
            doel_assets = filter_assets(assets=doel_assets, assettype=assettype)
        else:
            doel_assets = []
        if len(doel_assets) == 1:
            doel_asset = doel_assets[0]
            _ = create_relatie_if_missing(client=client, bron_asset=asset, doel_asset=doel_asset, relatie=relatie)
            if set_afgeleide_locatie:
                client.locatie_service.update_locatie(bron_asset=asset, doel_asset=doel_asset)


if __name__ == '__main__':
    configure_logger()
    logging.info('Kwaliteitscontrole van voeding-gerelateerde assets.\n'
                 'Toevoegen van relaties en locaties voor assettypes:\n'
                 '\n\tHS\n\tHSDeel\n\tHSCabine\n\tLS\n\tLSDeel\n\tAfstandsbewaking\n\tSegmentController')
    environment = Environment.PRD
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=load_settings())

    input_filepath = (Path.home() / 'Downloads' /
                      '[RSA] Locatie ontbreekt voor voeding-assets (LS, LSDeel, HS, HSDeel, HSCabine, '
                      'SegmentController, Afstandsbewaking).xlsx')
    df_assets_voeding = read_rsa_report(filepath=input_filepath,
                                        usecols=['uuid', 'assettype', 'toestand', 'naampad', 'naam',
                                                 'opmerkingen (blijvend)'])
    logging.debug('Filter assets op toestand: "in-gebruik", "in-ontwerp", "gepland"')
    df_assets_voeding = df_assets_voeding[df_assets_voeding["toestand"].isin(['in-gebruik', 'in-ontwerp', 'gepland'])]
    logging.debug('Filter assets met een naampad')
    df_assets_voeding = df_assets_voeding.dropna(subset=['naampad'])

    df_assets_ls = df_assets_voeding[df_assets_voeding["assettype"] == 'LS']
    df_assets_lsdeel = df_assets_voeding[df_assets_voeding["assettype"] == 'LSDeel']
    df_assets_hs = df_assets_voeding[df_assets_voeding["assettype"] == 'HS']
    df_assets_hsdeel = df_assets_voeding[df_assets_voeding["assettype"] == 'HSDeel']
    df_assets_hscabine = df_assets_voeding[df_assets_voeding["assettype"] == 'HSCabineLegacy']
    df_assets_segc = df_assets_voeding[df_assets_voeding["assettype"] == 'SegC']
    df_assets_ab = df_assets_voeding[df_assets_voeding["assettype"] == 'AB']

    assettype_kast = eminfra_client.assettype_service.get_assettype(assettype_uuid=ASSETTYPE_UUID_KAST)
    assettype_hscabine = eminfra_client.assettype_service.get_assettype(assettype_uuid=ASSETTYPE_UUID_HSCABINELEGACY)
    assettype_lsdeel = eminfra_client.assettype_service.get_assettype(assettype_uuid=ASSETTYPE_UUID_LSDEEL)
    assettype_hsdeel = eminfra_client.assettype_service.get_assettype(assettype_uuid=ASSETTYPE_UUID_HSDEEL)

    process_relatie_locatie(client=eminfra_client, df=df_assets_ls, assettype=assettype_kast,
                            relatie=RelatieEnum.BEVESTIGING, doel_asset_is_parent=True, set_afgeleide_locatie=True)
    process_relatie_locatie(client=eminfra_client, df=df_assets_lsdeel, assettype=assettype_kast,
                            relatie=RelatieEnum.BEVESTIGING, doel_asset_is_parent=True, set_afgeleide_locatie=True)
    # test dat hier alle doel-assets worden teruggegeven. parameter doel_asset_is_parent = False
    process_relatie_locatie(client=eminfra_client, df=df_assets_ls, assettype=assettype_lsdeel,
                            relatie=RelatieEnum.VOEDT, doel_asset_is_parent=False, set_afgeleide_locatie=False)

    process_relatie_locatie(client=eminfra_client, df=df_assets_hs, assettype=assettype_hscabine,
                            relatie=RelatieEnum.BEVESTIGING, doel_asset_is_parent=True, set_afgeleide_locatie=True)
    process_relatie_locatie(client=eminfra_client, df=df_assets_hsdeel, assettype=assettype_hscabine,
                            relatie=RelatieEnum.BEVESTIGING, doel_asset_is_parent=True, set_afgeleide_locatie=True)
    process_relatie_locatie(client=eminfra_client, df=df_assets_hs, assettype=assettype_hsdeel,
                            relatie=RelatieEnum.VOEDT, doel_asset_is_parent=False, set_afgeleide_locatie=False)
    process_relatie_locatie(client=eminfra_client, df=df_assets_hsdeel, assettype=assettype_lsdeel,
                            relatie=RelatieEnum.VOEDT, doel_asset_is_parent=False, set_afgeleide_locatie=False)

    process_relatie_locatie(client=eminfra_client, df=df_assets_segc, assettype=assettype_lsdeel,
                            relatie=RelatieEnum.BEVESTIGING, doel_asset_is_parent=False, set_afgeleide_locatie=True)
    process_relatie_locatie(client=eminfra_client, df=df_assets_ab, assettype=assettype_lsdeel,
                            relatie=RelatieEnum.BEVESTIGING, doel_asset_is_parent=False, set_afgeleide_locatie=True)
