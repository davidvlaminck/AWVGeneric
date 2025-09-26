import logging
from collections.abc import Generator
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Classes.ImplementatieElement.RelatieObject import RelatieObject
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation
from otlmow_converter.OtlmowConverter import OtlmowConverter
from UseCases.utils import load_settings
from UseCases.betrokkenerelaties.utils_betrokkenerelatie import build_betrokkenerelatie, get_bestaande_betrokkenerelaties

BASE_DIR = Path.home() / "OneDrive - Nordend/projects/AWV/OTL_Aanpassingen/Toezichter_update"
INPUT_FILE = BASE_DIR / "input" / "toezichter_mapping_link_OTL-Legacy_20250925.xlsx"
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
    'AWV_EW_WV': 'V&W West-Vlaanderen'
}

def map_toezichtsgroep(toezichtsgroep: str) -> str:
    """
    Map toezichtsgroep naam van LGC naar OTL
    :param toezichtsgroep: Legacy
    :return: naam toezichtsgroep OTL
    """
    return DICT_TOEZICHTSGROEPEN[toezichtsgroep]


def load_input(INPUT_FILE) -> pd.DataFrame:
    df = pd.read_excel(INPUT_FILE
                      , sheet_name='Toezichters'
                      , header=0
                      , usecols=["uri_otl", "uuid_otl", "naam_otl", "agent_uuid", "agent_naam", "gemeente", "provincie", "hoortBij-relatie", "uri_lgc", "uuid_lgc", "naam_lgc", "toezichter_uuid", "toezichter_naam", "toezichtgroep_uuid", "toezichtgroep_naam", "otl_lgc_link"])
    return df[df["otl_lgc_link"] == 1]


def process_assets(client: EMInfraClient, df: pd.DataFrame):
    existing, created = [], []
    for idx, row in df.iterrows():
        asset = next(client.search_asset_by_uuid(asset_uuid=row["uuid_otl"]))
        for role, name_col, mapper in [
            ("toezichter", "toezichter_naam", lambda n: n),
            ("toezichtsgroep", "toezichtgroep_naam", map_toezichtsgroep),
        ]:
            # deactivate
            existing += list(get_bestaande_betrokkenerelaties(client, asset, role, False))
            # create
            name = row[name_col]
            if not name: continue
            rel = build_betrokkenerelatie(client, asset, mapper(name), role)
            if rel:
                rel.assetId.identificator = f"HeeftBetrokkene_{idx}_{role}"
                created.append(rel)
    return existing, created


def write_output(existing, created, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    OtlmowConverter.from_objects_to_file(out_dir / "assets_delete.xlsx", existing)
    OtlmowConverter.from_objects_to_file(out_dir / "assets_update.xlsx", created)


def main():
    client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())
    df = load_input(INPUT_FILE)
    existing, created = process_assets(client, df)
    write_output(existing, created, OUTPUT_DIR)


main()