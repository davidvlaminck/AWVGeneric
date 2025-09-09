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

BASE_DIR = Path.home() / "OneDrive - Nordend/projects/AWV/OTL_Aanpassingen/Toezichter_update"
INPUT_FILE = BASE_DIR / "input" / "toezichter_mapping_link_OTL-Legacy_20250909.xlsx"
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

def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

def build_betrokkenerelatie(client: EMInfraClient, source: AssetDTO, agent_naam :str, rol: str) -> RelatieObject | None:
    generator_agents = client.get_objects_from_oslo_search_endpoint(
        url_part='agents'
        , filter_dict={"naam": agent_naam})
    agents = list(generator_agents)
    if len(agents) != 1:
        logging.debug('Agent was not found or returned multiple results.')
        return None
    agent_uri = agents[0].get('@type')
    agent_uuid = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]

    return create_betrokkenerelation(rol=rol
                                     , source_typeURI=source.type.uri
                                     , source_uuid=source.uuid
                                     , target_uuid=agent_uuid
                                     , target_typeURI=agent_uri)

def get_bestaande_betrokkenerelaties(client: EMInfraClient, asset: AssetDTO, rol: str, isActief: bool) -> Generator[RelatieObject]:
    generator = client.get_objects_from_oslo_search_endpoint(
        url_part='betrokkenerelaties'
        , filter_dict={"bronAsset": asset.uuid, 'rol': rol})

    for item in generator:
        betrokkenerelatie_uuid = item['RelatieObject.assetId']['DtcIdentificator.identificator']
        relatie = create_betrokkenerelation(
            rol=rol,
            source_typeURI=item['RelatieObject.bron']['@type'],
            source_uuid=item['RelatieObject.bronAssetId']['DtcIdentificator.identificator'][:36],
            target_typeURI=item['RelatieObject.doel']['@type'],
            target_uuid=item['RelatieObject.doelAssetId']['DtcIdentificator.identificator'][:36],
        )
        relatie.assetId.identificator = betrokkenerelatie_uuid  # Assign existing UUID
        relatie.isActief = isActief
        yield relatie

def map_toezichtsgroep(toezichtsgroep: str) -> str:
    """
    Map toezichtsgroep naam van LGC naar OTL
    :param toezichtsgroep: Legacy
    :return: naam toezichtsgroep OTL
    """
    return DICT_TOEZICHTSGROEPEN[toezichtsgroep]


def load_input(INPUT_FILE) -> pd.DataFrame:
    df = pd.read_excel(Path().home() / "OneDrive - Nordend/projects/AWV/OTL_Aanpassingen/Toezichter_update" / "input" /
                                        "toezichter_mapping_link_OTL-Legacy_20250909.xlsx"
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