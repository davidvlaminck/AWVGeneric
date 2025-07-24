import logging
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

print(
    """
    Aanmaken van een Bevestiging-Relatie tussen de Legacy assets SegmentController en LSDeel
    """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path


def read_excel_as_dataframe(filepath: Path, usecols: list[str]):
    """Read RSA-report as input into a DataFrame."""
    if usecols is None:
        usecols = ["uuid"]
    df_assets = pd.read_excel(filepath, sheet_name='Sheet1', header=0, usecols=usecols)
    df_assets = df_assets.dropna(subset=usecols)     # filter rows with NaN in specific columns
    return df_assets


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Aanmaken van een Bevestiging-Relatie tussen de Legacy assets SegmentController en LSDeel')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # Read input report
    df_assets = read_excel_as_dataframe(
        filepath=Path().home() / 'Downloads' / 'SegmentController' / 'SegmentControllers ontbrekende Bevestiging relatie.xlsx',
        usecols=["segc_uuid", "segc_naam", "lsdeel_uuid", "lsdeel_naam"])

    kenmerkType_uuid, relatieType_uuid = eminfra_client.get_kenmerktype_and_relatietype_id(relatie_uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')
    for idx, asset in df_assets.iterrows():
        # bestaande Bevestiging-relaties ophalen
        bestaande_relaties = eminfra_client.search_relaties(
            assetId=asset.get("segc_uuid")
            , kenmerkTypeId=kenmerkType_uuid
            , relatieTypeId=relatieType_uuid
        )

        # Als de relatie al bestaat, continue, ga uit de for-loop
        if next(bestaande_relaties, None):
            print(
                f'''Bevestiging-relatie reeds bestaande tussen SegmentController ({asset.get(
                    "segc_uuid")}) en LSDeel ({asset.get("lsdeel_uuid")})''')
            continue

        # Genereer nieuwe relatie (Legacy)
        bevestigingrelatie_uuid = eminfra_client.create_assetrelatie(
            bronAsset_uuid=asset.get("segc_uuid")
            , doelAsset_uuid=asset.get("lsdeel_uuid")
            , relatieType_uuid=relatieType_uuid
        )
        logging.debug(f'Bevestiging-relatie ({bevestigingrelatie_uuid}) aangemaakt tussen Segmentcontroller '
                    f'({asset.get("segc_uuid")}) en LSDeel ({asset.get("lsdeel_uuid")})')