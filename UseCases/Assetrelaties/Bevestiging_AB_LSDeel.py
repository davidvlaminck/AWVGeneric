import logging
from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import RelatieEnum
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from UseCases.utils import load_settings


def read_excel_as_dataframe(filepath: Path, usecols: list[str]):
    """Read RSA-report as input into a DataFrame."""
    if usecols is None:
        usecols = ["uuid"]
    df_assets = pd.read_excel(filepath, sheet_name='Sheet1', header=0, usecols=usecols)
    df_assets = df_assets.dropna(subset=usecols)     # filter rows with NaN in specific columns
    return df_assets


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Aanmaken van een Bevestiging-Relatie tussen de Legacy assets Afstandsbewaking en LSDeel')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # Read input report
    df_assets = read_excel_as_dataframe(
        filepath=Path().home() / 'Downloads' / 'Afstandsbewaking' / 'Afstandsbewaking ontbrekende Bevestiging relatie.xlsx',
        usecols = ["ab_uuid", "ab_naam", "lsdeel_uuid", "lsdeel_naam"])

    kenmerkType_uuid, relatieType_uuid = eminfra_client.get_kenmerktype_and_relatietype_id(relatie=RelatieEnum.BEVESTIGING)
    for idx, asset in df_assets.iterrows():
        # Afstandsbewaking
        # bestaande Bevestiging-relaties ophalen
        bestaande_relaties = eminfra_client.search_relaties(
            assetId=asset.get("ab_uuid")
            , kenmerkTypeId=kenmerkType_uuid
            , relatieTypeId=relatieType_uuid
        )

        # Als de relatie al bestaat, continue, ga uit de for-loop
        if next(bestaande_relaties, None):
            print(
                f'''Bevestiging-relatie reeds bestaande tussen Afstandsbewaking ({asset.get(
                    "ab_uuid")}) en LSDeel ({asset.get("lsdeel_uuid")})''')
            continue

        # Genereer nieuwe relatie (Legacy)
        asset_ab = eminfra_client.get_asset_by_id(asset_id=asset.get("ab_uuid"))
        asset_lsdeel = eminfra_client.get_asset_by_id(asset_id=asset.get("lsdeel_uuid"))
        bevestigingrelatie_uuid = eminfra_client.create_assetrelatie(bronAsset=asset_ab
            , doelAsset=asset_lsdeel
            , relatie=RelatieEnum.BEVESTIGING
        )
        logging.debug(f'Bevestiging-relatie ({bevestigingrelatie_uuid}) aangemaakt tussen Afstandsbewaking '
                    f'({asset.get("ab_uuid")}) en LSDeel ({asset.get("lsdeel_uuid")})')