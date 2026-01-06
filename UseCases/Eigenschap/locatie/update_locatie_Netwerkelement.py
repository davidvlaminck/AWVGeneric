import logging

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from utils.wkt_geometry_helpers import format_locatie_kenmerk_lgc_2_wkt, geometries_are_identical


def load_settings():
    """Load API settings from JSON"""
    return (
        Path().home()
        / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    )

def read_excel_as_dataframe(filepath: Path, usecols: list[str]):
    """Read RSA-report as input into a DataFrame."""
    if usecols is None:
        usecols = ["uuid"]
    return pd.read_excel(
        filepath, sheet_name='Netwerkelement', header=0, usecols=usecols
    )

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

        MAPPING = {
            'https://lgc.data.wegenenverkeer.be/ns/installatie#Kast': 'Kast',
            'https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel': 'LSDeel',
            'https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy': 'HSCabine',
            'https://lgc.data.wegenenverkeer.be/ns/installatie#GebouwLegacy': 'GebouwLegacy',
        }

        for uri, label in MAPPING.items():
            rels = [
                r for r in bestaande_relaties_bevestiging
                if r.type.get('uri') == uri
            ]
            if len(rels) == 1:
                loc_kenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(rels[0].uuid)
                wkt_geom = format_locatie_kenmerk_lgc_2_wkt(loc_kenmerk)
                if not geometries_are_identical(asset.get('geometry'), wkt_geom):
                    df_assets.loc[idx, ['geometry_new', 'assettype']] = [wkt_geom, label]
                break
        else:
            # no single match found
            df_assets.loc[idx, ['geometry_new', 'assettype']] = ['onbepaald', 'manuele inspectie']

    # #################################################################################
    # ####  Write to DAVIE-compliant file
    # #################################################################################
    file_path = Path().home() / 'Downloads' / 'Netwerkelement' / 'DA-2025-XXXXX_import.xlsx'
    print(f'Write DAVIE file to: {file_path}')
    df_assets.to_excel(file_path, freeze_panes=[1,2], index=False, sheet_name='Netwerkelement')
