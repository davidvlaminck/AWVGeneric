from collections.abc import Generator
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Classes.ImplementatieElement.RelatieObject import RelatieObject
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation
from otlmow_converter.OtlmowConverter import OtlmowConverter

def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

def read_report(downloads_subpath: str, sheet_name: str = 'Resultaat', usecols: list = ["uuid"]):
    """Read RSA-report as input into a DataFrame."""
    filepath = Path().home() / 'Downloads' / downloads_subpath
    df_assets = pd.read_excel(filepath, sheet_name=sheet_name, header=2, usecols=usecols)
    df_assets = df_assets.where(pd.notna(df_assets), None)
    df_assets.fillna(value='', inplace=True)
    df_assets.drop_duplicates(inplace=True)
    return df_assets

def construct_full_name(first_name: str, last_name: str) -> str | None:
    return " ".join([first_name, last_name]) if first_name and last_name else None

def build_betrokkenerelatie(source: AssetDTO, agent_naam :str, rol: str) -> RelatieObject | None:
    generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(
        url_part='agents'
        , filter_dict={"naam": agent_naam})
    agents = list(generator_agents)
    if len(agents) != 1:
        print('Agent was not found or returned multiple results.')
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
            
def map_toezichtgroep(existing_toezichtgroep, json_file = 'mapping_toezichtsgroepen.json') -> str:
    """
    Maps the input value to a new value, based on a mapping file
    If the record is missing in the mapping-file, returns the input.
    
    :param existing_toezichtgroep: the actual value that will be mapped
    :param json_file: external mapping file, in a json-structure
    :return: the mapped value, or the initial value if the mapping can not be effected
    """
    df_mapping = pd.read_json(json_file)

    return next(
        iter(
            df_mapping.loc[
                df_mapping['toezichtsgroep_existing'] == existing_toezichtgroep,
                'toezichtsgroep_new',
            ].values
        ),
        existing_toezichtgroep,
    )

dict_toezichtsgroepen = {
    "V&W-WA": "V&W Antwerpen",
    "V&W-WVB": "V&W Vlaams-Brabant",
    "V&W-WL": "V&W Limburg",
    "V&W-WO": "V&W Oost-Vlaanderen",
    "V&W-WW": "V&W West-Vlaanderen",
    "EMT_TELE": "EMT_TELE",
    "EMT_VHS": "EMT_VHS"
}

def map_toezichtsgroep(toezichtsgroep: str) -> str:
    """
    Map toezichtsgroep naam van LGC naar OTL
    :param toezichtsgroep: Legacy
    :return: naam toezichtsgroep OTL
    """
    return dict_toezichtsgroepen[toezichtsgroep]


if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # assettype = 'Signaalkabel'
    # assettype = 'Voedingskabel'
    assettype = 'Beschermbuis'
    df_assets = read_report(
        downloads_subpath=f'toezichter/input/[RSA] Bijhorende assets hebben een verschillende toezichtshouder (assettype = {assettype}).xlsx',
        usecols=["otl_uuid", "otl_uri", "lgc_uuid", "lgc_toezichthouder_gebruikersnaam", "lgc_toezichtsgroep_naam",
                 "lgc_toezichthouder_voornaam", "lgc_toezichthouder_naam"])

    existing_assets = []
    created_assets = []
    for index, asset in df_assets.iterrows():
        print(f'Processing asset: {asset.otl_uuid}')
        #################################################################################
        ####  Ophalen van de bestaande asset
        #################################################################################
        otl_asset = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset.otl_uuid))

        #################################################################################
        ####  Wis de bestaande betrokkenerelatie. Set isActief = False
        #################################################################################
        print('\tListing existing relations toezichter and toezichtsgroep')
        bestaande_relatie_toezichter = list(get_bestaande_betrokkenerelaties(asset=otl_asset, rol='toezichter', isActief=False))
        # existing_assets.extend(
        #     list(get_bestaande_betrokkenerelaties(asset=otl_asset, rol='toezichter', isActief=False)))

        bestaande_relatie_toezichtsgroep = list(get_bestaande_betrokkenerelaties(asset=otl_asset, rol='toezichtsgroep', isActief=False))
        # existing_assets.extend(
        #     list(get_bestaande_betrokkenerelaties(asset=otl_asset, rol='toezichtsgroep', isActief=False)))

        #################################################################################
        ####  Maak nieuwe BetrokkeneRelaties
        #################################################################################
        print('\tCreating new relations toezichter and toezichtsgroep')
        # search toezichter and extract the uuid of the toezichter. This ensures that the toezichter exists.
        toezichter_naam = construct_full_name(first_name=asset.lgc_toezichthouder_voornaam,
                                              last_name=asset.lgc_toezichthouder_naam)
        toezichtgroep_naam = asset.lgc_toezichtsgroep_naam

        if toezichter_naam:
            print(f'\t\tToezichter: {toezichter_naam}')
            nieuwe_relatie_toezichter = build_betrokkenerelatie(source=otl_asset, agent_naam=toezichter_naam, rol='toezichter')
            if nieuwe_relatie_toezichter is None:
                continue
            nieuwe_relatie_toezichter.assetId.identificator = f'HeeftBetrokkene_{index}_toezichter_{assettype}'


        if toezichtgroep_naam:
            toezichtgroep_naam = map_toezichtgroep(toezichtgroep_naam)

            print(f'\t\tToezichtsgroep: {toezichtgroep_naam}')
            nieuwe_relatie_toezichtsgroep = build_betrokkenerelatie(source=otl_asset, agent_naam=map_toezichtsgroep(toezichtgroep_naam),
                                                                    rol='toezichtsgroep')
            if nieuwe_relatie_toezichtsgroep is None:
                continue
            nieuwe_relatie_toezichtsgroep.assetId.identificator = f'HeeftBetrokkene_{index}_toezichtsgroep_{assettype}'

        existing_assets.extend(bestaande_relatie_toezichter)
        existing_assets.extend(bestaande_relatie_toezichtsgroep)
        created_assets.extend(
            (nieuwe_relatie_toezichter, nieuwe_relatie_toezichtsgroep)
        )
    OtlmowConverter.from_objects_to_file(file_path=Path(Path().home() / 'Downloads' / 'toezichter' / 'output' / f'{assettype}' / f'assets_delete_toezichter_toezichtsgroep_{assettype}.xlsx'),
                                         sequence_of_objects=existing_assets)
    OtlmowConverter.from_objects_to_file(file_path=Path(Path().home() / 'Downloads' / 'toezichter' / 'output' / f'{assettype}' / f'assets_update_toezichter_toezichtsgroep_{assettype}.xlsx'),
                                         sequence_of_objects=created_assets)