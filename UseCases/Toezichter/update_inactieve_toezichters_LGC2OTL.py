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

def build_betrokkenerelatie(source: AssetDTO, agent_naam :str, rol: str) -> RelatieObject | None:
    generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(
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

def get_bestaande_betrokkenerelaties(asset: AssetDTO, rol: str, isActief: bool) -> Generator[RelatieObject]:
    generator = eminfra_client.get_objects_from_oslo_search_endpoint(
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


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('''
        OTL Toezichters en toezichtsgroepen aanpassen.
        Op basis van de actuele LGC toezichter en LGC toezichtsgroep, de OTL Agent rol: toezichter en OTL Agent rol: toezichtsgroep updaten.
        We beschouwen de Legacy informatie als correct en passen de OTL-informatie daaraan aan.
        Input: Assets die en inactieve toezichter hebben: Martin Van Leuven of Maurits Van Overloop.
        Output: DAVIE-conforme aanlever bestanden om de huidige Agents (toezichter en toezichtsgroep) te deactiveren en een nieuwe te instantiëren. 
        ''')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = pd.read_excel(Path().home() / "OneDrive - Nordend/projects/AWV/OTL_Aanpassingen/Toezichter_update" / "input" /
                                        "toezichter_mapping_link_OTL-Legacy_20250909.xlsx"
                              , sheet_name='Toezichters'
                              , header=0
                              , usecols=["uri_otl", "uuid_otl", "naam_otl", "agent_uuid", "agent_naam", "gemeente", "provincie", "hoortBij-relatie", "uri_lgc", "uuid_lgc", "naam_lgc", "toezichter_uuid", "toezichter_naam", "toezichtgroep_uuid", "toezichtgroep_naam", "otl_lgc_link"])

    logging.info('Verwijder de records waarbij kolom "otl_lgc_link" niet gelijk is aan 1.')
    df_assets = df_assets[df_assets["otl_lgc_link"] == 1]

    existing_assets = []
    created_assets = []
    for index, asset in df_assets.iterrows():
        logging.info(f'Processing asset {index}: {asset["uuid_otl"]}')
        #################################################################################
        ####  Ophalen van de bestaande asset
        #################################################################################
        otl_asset = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset["uuid_otl"]))

        #################################################################################
        ####  Wis de bestaande betrokkenerelatie. Set isActief = False
        #################################################################################
        logging.info('\tListing existing relations toezichter and toezichtsgroep')
        bestaande_relatie_toezichter = list(get_bestaande_betrokkenerelaties(asset=otl_asset, rol='toezichter', isActief=False))
        if bestaande_relatie_toezichter:
            existing_assets.extend(bestaande_relatie_toezichter)
        bestaande_relatie_toezichtsgroep = list(get_bestaande_betrokkenerelaties(asset=otl_asset, rol='toezichtsgroep', isActief=False))
        if bestaande_relatie_toezichtsgroep:
            existing_assets.extend(bestaande_relatie_toezichtsgroep)

        #################################################################################
        ####  Maak nieuwe BetrokkeneRelaties
        #################################################################################
        logging.info('\tCreating new relations toezichter and toezichtsgroep')
        # search toezichter and extract the uuid of the toezichter. This ensures that the toezichter exists.
        toezichter_naam = asset['toezichter_naam']
        toezichtgroep_naam = asset['toezichtgroep_naam']

        if toezichter_naam:
            logging.info(f'\t\tToezichter: {toezichter_naam}')
            nieuwe_relatie_toezichter = build_betrokkenerelatie(source=otl_asset, agent_naam=toezichter_naam, rol='toezichter')
            if nieuwe_relatie_toezichter is None:
                continue
            nieuwe_relatie_toezichter.assetId.identificator = f'HeeftBetrokkene_{index}_toezichter'
            created_assets.extend([nieuwe_relatie_toezichter])

        if toezichtgroep_naam:
            logging.info(f'\t\tToezichtsgroep: {toezichtgroep_naam}')
            nieuwe_relatie_toezichtsgroep = build_betrokkenerelatie(source=otl_asset, agent_naam=map_toezichtsgroep(toezichtgroep_naam),
                                                                    rol='toezichtsgroep')
            if nieuwe_relatie_toezichtsgroep is None:
                continue
            nieuwe_relatie_toezichtsgroep.assetId.identificator = f'HeeftBetrokkene_{index}_toezichtsgroep'
            created_assets.extend([nieuwe_relatie_toezichtsgroep])

    OtlmowConverter.from_objects_to_file(file_path=Path(Path().home() / "OneDrive - Nordend/projects/AWV"
                                                        "/OTL_Aanpassingen/Toezichter_update" / 'output' / f'assets_delete_toezichter_toezichtsgroep.xlsx'),
                                         sequence_of_objects=existing_assets)
    OtlmowConverter.from_objects_to_file(file_path=Path(Path().home() / "OneDrive - Nordend/projects/AWV"
                                                        "/OTL_Aanpassingen/Toezichter_update" / 'output' / f'assets_update_toezichter_toezichtsgroep.xlsx'),
                                         sequence_of_objects=created_assets)