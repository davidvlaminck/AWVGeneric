import logging

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_converter.OtlmowConverter import OtlmowConverter
from UseCases.utils import load_settings
from UseCases.betrokkenerelaties.utils_betrokkenerelatie import build_betrokkenerelatie, get_bestaande_betrokkenerelaties

BASE_DIR = Path.home() / "OneDrive - Nordend/projects/AWV/OTL_Aanpassingen/toezichter"
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"


DICT_TOEZICHTSGROEPEN = {
    "V&W-WA": "V&W Antwerpen",
    "V&W-WVB": "V&W Vlaams-Brabant",
    "V&W-WL": "V&W Limburg",
    "V&W-WO": "V&W Oost-Vlaanderen",
    "V&W-WW": "V&W West-Vlaanderen",
    "EMT_TELE": "EMT_TELE",
    "EMT_VHS": "EMT_VHS",
    "Tunnel Organ. VL.": "Afdeling Tunnelorganisatie",
    'Afdeling Wegen en Verkeer West-Vlaanderen': "V&W West-Vlaanderen",
    'AWV_414_SINT-NIKLAAS': 'District St-Niklaas 414',
    'EMT_WHA': 'EMT_WHA',
    'EMT_WHG': 'EMT_WHG',
    'EMT_WHM': 'EMT_WHM',
    'EMT_WHO': 'EMT_WHO',
    'EMT_WHW': 'EMT_WHW',
    'PCO': 'PCO',
    'AWV_EW_WV': 'V&W West-Vlaanderen',
    'LANTIS': 'LANTIS'
}


def read_report(filepath: str, sheet_name: str = 'Resultaat', usecols: list = ["uuid"]):
    """Read RSA-report as input into a DataFrame."""
    df_assets = pd.read_excel(filepath, sheet_name=sheet_name, header=2, usecols=usecols)
    df_assets = df_assets.where(pd.notna(df_assets), None)
    df_assets.fillna(value='', inplace=True)
    df_assets.drop_duplicates(inplace=True)
    return df_assets

def map_toezichtsgroep_lgc2otl(toezichtsgroep: str) -> str:
    """
    Map toezichtsgroep naam van LGC naar OTL
    :param toezichtsgroep: Legacy
    :return: naam toezichtsgroep OTL
    """
    return DICT_TOEZICHTSGROEPEN[toezichtsgroep]


def process_assets(client: EMInfraClient, df: pd.DataFrame):
    """
    Process a pandas Dataframe of assets containing a Legacy toezichter and a Legacy toezichtsgroep.
    Returns 2 lists: "existing" and "created" with the existing and to-be-created OTL-HeeftBetrokkene relaties respectively.

    When the existing, or the new-to-be-created HeeftBetrokkene relaties is missing for an asset, none is appended to the list,
    to ensure that an existing betrokkenerelatie is not deactivated without creating a new one.

    This is checked for both "toezichter" and "toezichtsgroep".

    :param client:
    :param df:
    :return:
    """
    existing, created = [], []
    for idx, row in df.iterrows():
        asset = next(client.search_asset_by_uuid(asset_uuid=row["otl_uuid"]))
        logging.info(f'Processing asset:\n\t{idx}\n\t{asset.uuid}')
        for role, name_col, mapper in [
            ("toezichter", "lgc_toezichter_naam", lambda naam: naam),
            ("toezichtsgroep", "lgc_toezichtsgroep_naam", map_toezichtsgroep_lgc2otl),
        ]:
            # deactivate
            rel_existing = list(get_bestaande_betrokkenerelaties(client, asset, role, False))
            # create
            name = row[name_col]
            if not name:
                continue
            rel_new = build_betrokkenerelatie(client, asset, mapper(name), role)

            # rel_existing can be None, but rel_new must exist.
            # To ensure the existing is not deactivated, without adding a new toezichter or toezichtsgroep.
            if rel_new:
                existing += rel_existing
                rel_new.assetId.identificator = f"HeeftBetrokkene_{idx}_{role}"
                created.append(rel_new)
            if rel_existing and not rel_new:
                logging.critical(f'Asset {asset.uuid} has existing toezichter: {rel_existing}')
                logging.critical(f'New toezichter not found: {rel_new}')
    return existing, created

def write_output(existing, created, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    OtlmowConverter.from_objects_to_file(out_dir / "assets_delete.xlsx", existing)
    OtlmowConverter.from_objects_to_file(out_dir / "assets_update.xlsx", created)

def main():
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                filemode="w")
    client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())
    for assettype in ['Signaalkabel', 'Voedingskabel', 'Beschermbuis', 'WVLichtmast']:
        df = read_report(
            INPUT_DIR / f'[RSA] Bijhorende assets hebben een verschillende toezichtshouder (assettype = {assettype}).xlsx'
            , usecols=["otl_uuid", "otl_uri", "lgc_uuid", "lgc_toezichter_naam", "lgc_toezichtsgroep_naam"]
        )
        existing, created = process_assets(client, df)
        write_output(existing=existing, created=created, out_dir=Path(OUTPUT_DIR, f'{assettype}'))

main()