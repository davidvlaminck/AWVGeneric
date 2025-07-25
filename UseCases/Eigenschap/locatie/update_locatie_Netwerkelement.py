import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import LocatieKenmerk
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

def load_settings():
    """Load API settings from JSON"""
    return (
        Path().home()
        / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    )

def format_locatie_kenmerk_lgc_2_wkt(locatie: LocatieKenmerk) -> str:
    """
    Format LocatieKenmerk as input to a WKT string as output
    Supported geometry formats: Point
    :param locatie: LocatieKenmerk 
    :return: 
    """
    locatie_data = getattr(locatie, "locatie", None)
    if not locatie_data or locatie_data.get("geometrie") is None:
        return None
    if locatie.locatie.get('_type') != 'punt':
        # implementation for other geometry types
        return None
    coordinaten = locatie.locatie.get('coordinaten')
    return f'POINT Z ({coordinaten.get("x")} {coordinaten.get("y")} {coordinaten.get("z", 0)})'

def parse_x_coordinate(wkt_geom: str) -> str:
    """
    Returns an integer X-coordinate of a wkt-geometry string
    :param wkt_geom:
    :return: integer x-coordinate
    """
    if wkt_geom == 'nan' or pd.isna(wkt_geom):
        return None

    # Extract the numbers from the parentheses
    coordinates = wkt_geom.split('(')[1].split(')')[0].split()

    # Get the first (X) coordinate and convert to int
    return str(int(float(coordinates[0])))

def geometries_are_identical(wkt_geom1, wkt_geom2) -> bool:
    """
    Compares two Points, wkt geometry. Returns true if they are identical.

    :param wkt_geom1:
    :param wkt_geom2:
    :return:
    """
    x_coord_1 = parse_x_coordinate(wkt_geom1)
    x_coord_2 = parse_x_coordinate(wkt_geom2)
    return x_coord_1 == x_coord_2

def read_excel_as_dataframe(filepath: Path, usecols: list[str]):
    """Read RSA-report as input into a DataFrame."""
    if usecols is None:
        usecols = ["uuid"]
    df_assets = pd.read_excel(filepath, sheet_name='Netwerkelement', header=0, usecols=usecols)
    # df_assets = df_assets.dropna(subset=usecols)     # filter rows with NaN in specific columns
    return df_assets

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info(
        """Update de locatie van Netwerkelement (OTL)
        Selecteer Netwerkelement waar de eigenschap gebruik niet gelijk is aan "CEN"; "OTN"; "SDH"
        Neem de locatie van het asset waarmee het Netwerkelement verbonden is (via Bevestiging-relatie).
        Indien de verbonden asset geen locatie heeft, manuele inspectie.
        Indien de locatie verschilt > neem de nieuwe locatie over van de verbonden asset.
        
        Voorbeeld: 
        https://apps.mow.vlaanderen.be/eminfra/assets/144cc6a9-7668-4e53-b76d-61247c86654d
         """
    )

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    excel_path = Path().home() / 'Downloads' / 'Netwerkelement' / 'DA-2025-43389_export_selectie.xlsx'
    # Read input report
    df_assets = read_excel_as_dataframe(
        filepath=excel_path,
        usecols=["typeURI", "assetId.identificator", "beschrijvingFabrikant", "datumOprichtingObject", "gebruik",
                 "ipAddressBeheer", "ipAddressMask", "ipGateway", "isActief", "merk", "modelnaam", "nSAPAddress",
                 "naam", "naampad", "notitie", "serienummer", "softwareVersie", "telefoonnummer",
                 "theoretischeLevensduur", "toestand", "geometry"]
    )

    kenmerkType_uuid_bevestiging, relatieType_uuid_bevestiging = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')

    logging.debug('Ophalen van de locatie van een Legacy asset die via de HoortBij-relatie met een Netwerkelement is verbonden')
    df_assets["geometry_new"] = None # Append a new column
    df_assets["assettype"] = None
    for idx, asset in df_assets.iterrows():
        logging.debug(f"Asset: {idx}: processing asset Netwerkelement: {asset.get('assetId.identificator')}")

        # bestaande relaties ophalen: Bevestiging naar een Kast
        bestaande_relaties_bevestiging = eminfra_client.search_relaties(
            assetId=asset.get("assetId.identificator")[:36]
            , kenmerkTypeId=kenmerkType_uuid_bevestiging
            , relatieTypeId=relatieType_uuid_bevestiging
        )
        bestaande_relaties_bevestiging = list(bestaande_relaties_bevestiging)
        bestaande_relaties_bevestiging_met_kast = [i for i in bestaande_relaties_bevestiging if i.type.get('uri') == 'https://lgc.data.wegenenverkeer.be/ns/installatie#Kast']
        bestaande_relaties_bevestiging_met_lsdeel = [i for i in bestaande_relaties_bevestiging if i.type.get('uri') == 'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel']
        bestaande_relaties_bevestiging_met_hscabine = [i for i in bestaande_relaties_bevestiging if i.type.get('uri') == 'https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy']
        bestaande_relaties_bevestiging_met_gebouwlegacy = [i for i in bestaande_relaties_bevestiging if i.type.get('uri') == 'https://lgc.data.wegenenverkeer.be/ns/installatie#GebouwLegacy']

        logging.debug(f'Er zijn exact {len(bestaande_relaties_bevestiging_met_kast)} Bevestigings-relaties van een Netwerkelement met een Kast beschikbaar')
        if len(bestaande_relaties_bevestiging_met_kast) == 1:
            logging.debug('Exact 1 bevestigings-relaties met een Kast beschikbaar')
            locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(bestaande_relaties_bevestiging_met_kast[0].uuid)
            wkt_geom = format_locatie_kenmerk_lgc_2_wkt(locatiekenmerk)

            logging.debug('Update geometrie van het Netwerkelement en de Kast.')
            logging.debug(asset.get('geometry'))
            logging.debug(wkt_geom)
            if not geometries_are_identical(asset.get('geometry'), wkt_geom):
                df_assets.loc[idx, "geometry_new"] = wkt_geom
                df_assets.loc[idx, "assettype"] = 'Kast'

        elif len(bestaande_relaties_bevestiging_met_lsdeel) == 1:
            logging.debug('Exact 1 bevestigings-relaties met een LSDeel beschikbaar')
            locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(bestaande_relaties_bevestiging_met_lsdeel[0].uuid)
            wkt_geom = format_locatie_kenmerk_lgc_2_wkt(locatiekenmerk)

            logging.debug('Update geometrie van het Netwerkelement en het LSDeel.')
            if not geometries_are_identical(asset.get('geometry'), wkt_geom):
                df_assets.loc[idx, "geometry_new"] = wkt_geom
                df_assets.loc[idx, "assettype"] = 'LSDeel'

        elif len(bestaande_relaties_bevestiging_met_hscabine) == 1:
            logging.debug('Exact 1 bevestigings-relaties met een HSCabine beschikbaar')
            locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(bestaande_relaties_bevestiging_met_hscabine[0].uuid)
            wkt_geom = format_locatie_kenmerk_lgc_2_wkt(locatiekenmerk)

            logging.debug('Update geometrie van het Netwerkelement en HSCabine.')
            if not geometries_are_identical(asset.get('geometry'), wkt_geom):
                df_assets.loc[idx, "geometry_new"] = wkt_geom
                df_assets.loc[idx, "assettype"] = 'HSCabine'

        elif len(bestaande_relaties_bevestiging_met_gebouwlegacy) == 1:
            logging.debug('Exact 1 bevestigings-relaties met een GebouwLegacy beschikbaar')
            locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(bestaande_relaties_bevestiging_met_gebouwlegacy[0].uuid)
            wkt_geom = format_locatie_kenmerk_lgc_2_wkt(locatiekenmerk)

            logging.debug('Update geometrie van het Netwerkelement en GebouwLegacy.')
            if not geometries_are_identical(asset.get('geometry'), wkt_geom):
                df_assets.loc[idx, "geometry_new"] = wkt_geom
                df_assets.loc[idx, "assettype"] = 'GebouwLegacy'

        else:
            df_assets.loc[idx, "geometry_new"] = 'onbepaald'
            df_assets.loc[idx, "assettype"] = 'manuele inspectie'
            continue


    # #################################################################################
    # ####  Write to DAVIE-compliant file
    # #################################################################################
    file_path = Path().home() / 'Downloads' / 'Netwerkelement' / 'DA-2025-XXXXX_import.xlsx'
    print(f'Write DAVIE file to: {file_path}')
    df_assets.to_excel(file_path, freeze_panes=[1,2], index=False, sheet_name='Netwerkelement')
