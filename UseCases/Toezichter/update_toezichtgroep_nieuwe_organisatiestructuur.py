from collections.abc import Generator
from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, QueryDTO, PagingModeEnum, ExpressionDTO, SelectionDTO, TermDTO, OperatorEnum, \
    AgentDTO, ExpansionsDTO
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Classes.ImplementatieElement.RelatieObject import RelatieObject
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation
from otlmow_converter.OtlmowConverter import OtlmowConverter

from Generic.ExcelModifier import ExcelModifier


def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

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

def get_bestaande_betrokkenerelaties(asset: AssetDTO, rol: str = None, agent: AgentDTO = None) -> Generator[RelatieObject]:
    filter_dict = {"bronAsset": asset.uuid}
    if rol:
        filter_dict['rol'] = rol
    if agent:
        filter_dict['doelAgent'] = agent.uuid
    generator = eminfra_client.get_objects_from_oslo_search_endpoint(
        url_part='betrokkenerelaties'
        , filter_dict=filter_dict)

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
        yield relatie

def _get_locatie_kenmerk_recursive(asset):
    if hasattr(asset, 'uuid') and asset.uuid:
        try:
            locatieKenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset.uuid)
            if hasattr(locatieKenmerk, 'locatie') and locatieKenmerk.locatie is None and hasattr(asset, 'parent') and asset.parent:
                return _get_locatie_kenmerk_recursive(asset.parent)
            else:
                return locatieKenmerk
        except (RuntimeError, ProcessLookupError):  # Replace with the actual exception thrown on failure
            if hasattr(asset, 'parent') and asset.parent:  # Ensure asset has a parent
                return _get_locatie_kenmerk_recursive(asset.parent)  # Recursive call with parent
            else:
                return None  # No parent left, return None
    else:
        return None

def log_error(uuid: str, message: str = 'Locatie ontbreekt'):
    global log_df
    log_df = pd.concat([log_df, pd.DataFrame([{
        "uuid": uuid,
        "message": message
    }])], ignore_index=True)

def log_error_missing_locatie(typeURI: str, assetId_identificator: str, bron_typeURI: str, bronAssetId_identificator: str, doel_typeURI: str, doelAssetId_identificator: str):
    global log_df_missing_locatie
    log_df_missing_locatie = pd.concat([log_df_missing_locatie, pd.DataFrame([
        {
            "typeURI": typeURI,
            "assetId.identificator": assetId_identificator,
            "bron.typeURI": bron_typeURI,
            "bronAssetId.identificator": bronAssetId_identificator,
            "doel.typeURI":doel_typeURI,
            "doelAssetId.identificator": doelAssetId_identificator
        }])], ignore_index=True)

provincie_to_toezichtsgroep = {
    'Antwerpen': "V&W-WA",
    'Vlaams-Brabant': "V&W-WVB",
    'Brussel': "V&W-WVB",
    'Limburg': "V&W-WL",
    'Oost-Vlaanderen': "V&W-WO",
    'West-Vlaanderen': "V&W-WW"
}

def map_toezichtsgroep(asset: AssetDTO) -> str:
    """
    Map toezichtsgroep Verkeershandhavingssysteem naar een nieuwe toezichtsgroep in functie van de provincie waarbinnen de asset is gesitueerd.
    Indien de locatie van de asset geen resultaten oplevert, zoek dan naar de locatie van diens parent.
    :return:
    """
    locatieKenmerk = _get_locatie_kenmerk_recursive(asset=asset)
    if hasattr(locatieKenmerk, 'locatie') and locatieKenmerk.locatie and 'adres' in locatieKenmerk.locatie and 'provincie' in locatieKenmerk.locatie.get('adres'):
        provincie = locatieKenmerk.locatie.get('adres').get('provincie')
        return provincie_to_toezichtsgroep.get(provincie)
    return None

agent_to_toezichtsgroep = {
    'Antwerpen': "V&W Antwerpen",
    'Vlaams-Brabant': "V&W Vlaams-Brabant",
    'Brussel': "V&W Vlaams-Brabant",
    'Limburg': "V&W Limburg",
    'Oost-Vlaanderen': "V&W Oost-Vlaanderen",
    'West-Vlaanderen': "V&W West-Vlaanderen"
}

def map_agent(asset: AssetDTO) -> str:
    """
    Map de agent Verkeershandhavingssysteem naar een nieuwe toezichtsgroep in functie van de provincie waarbinnen de asset is gesitueerd.
    Return None indien de locatie niet achterhaald kan worden (Rand Vlaadnderen-Brussel of aan buiten Vlaanderen)
    :return:
    """
    try:
        locatieKenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset.uuid)
        provincie = locatieKenmerk.locatie.get('adres').get('provincie')
        return agent_to_toezichtsgroep.get(provincie)
    except AttributeError:
        return None


def format_new_row(asset: AssetDTO, toezichtgroep_naam: str) -> dict:
    """Format information to a new dataframe row"""
    # ophalen toezichter
    toezichter_kenmerk = eminfra_client.get_kenmerk_toezichter_by_asset_uuid(asset_uuid=asset.uuid)
    toezichter = toezichter_kenmerk.toezichter
    if toezichter:
        toezichter_uuid = toezichter.get('uuid')
        toezichter = eminfra_client.get_identiteit(toezichter_uuid=toezichter_uuid)
        toezichter_gebruikersnaam = toezichter.gebruikersnaam
    else:
        toezichter_gebruikersnaam = None

    data = {}
    if asset:
        data["id"] = asset.uuid
        data["type"] = asset.type.korteUri
        data["actief"] = 'ja'
    data["toezicht|toezichtgroep"] = toezichtgroep_naam
    data["toezicht|toezichter"] = toezichter_gebruikersnaam
    return data

def append_row(df, new_data):
    """
    Appends a new row to an existing DataFrame.

    :param df: pd.DataFrame - The existing DataFrame
    :param new_data: dict - A dictionary containing the new row data (keys must match column names)
    :return: pd.DataFrame - Updated DataFrame with the new row
    """
    new_row = pd.DataFrame([new_data])
    return pd.concat([df, new_row], ignore_index=True)

def _deactivate_status(item):
    item.isActief = False
    return item

if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # Initialize empty DataFrame for logging
    log_df = pd.DataFrame(columns=["uuid", "message"])
    log_df_missing_locatie = pd.DataFrame(columns=["typeURI", "assetId.identificator", "bron.typeURI", "bronAssetId.identificator", "doel.typeURI", "doelAssetId.toegekendDoor"])

    excel_path = Path().home() / 'Downloads' / 'map_toezichtsgroep'

    ######################################
    ### LGC
    ######################################
    # read the complete mapping file (json) in a pandas dataframe
    df_mapping_toezichtsgroepen = pd.read_json('mapping_toezichtsgroepen.json')

    # loop over de "toezichtgroepen"
    for _, toezichtsgroep_map in df_mapping_toezichtsgroepen.iterrows():
        toezichtgroep_bestaand = next(eminfra_client.search_toezichtgroep_lgc(naam=toezichtsgroep_map.toezichtsgroep_existing), None)
        toezichtgroep_nieuw = next(eminfra_client.search_toezichtgroep_lgc(naam=toezichtsgroep_map.toezichtsgroep_new), None)

        if toezichtgroep_bestaand and toezichtgroep_nieuw:
            print(f'Map all assets from toezichtsgroep: {toezichtgroep_bestaand.naam} to {toezichtgroep_nieuw.naam}')
            print(f'Processing LGC assets (toezichtsgroep={toezichtgroep_bestaand.naam})')
            query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 expansions=ExpansionsDTO(fields=['parent']),
                                 selection=SelectionDTO(
                                     expressions=[ExpressionDTO(
                                         terms=[TermDTO(property='toezichtGroep', operator=OperatorEnum.EQ,
                                                        value=toezichtgroep_bestaand.uuid)])]))
            assets = eminfra_client.search_assets(query_dto=query_dto)
            df_assets_lgc = pd.DataFrame(
                columns=["id", "type", "actief", "toezicht|toezichter", "toezicht|toezichtgroep"])
            for idx, asset in enumerate(assets):
                print(f'\tProcessing asset: {asset.uuid}')
                # map toezichtsgroep Verkeershandhavingssystemen (EMT_VHS)
                toezichtgroep_naam = map_toezichtsgroep(asset) if toezichtgroep_nieuw.naam == 'EMT_VHS' else toezichtgroep_nieuw.naam
                if toezichtgroep_naam is None:
                    log_error(asset.uuid)
                else:
                    new_row_lgc_asset = format_new_row(asset=asset, toezichtgroep_naam=toezichtgroep_naam)
                    df_assets_lgc = append_row(df_assets_lgc, new_row_lgc_asset)

            ######################################
            ### Wegschrijven van de LGC-data per 1000 assets
            ######################################
                if (idx+1) % 1000 == 0:
                    df_assets_lgc.to_excel(
                        excel_path / f'map_toezichtsgroep_lgc_{toezichtgroep_bestaand.naam}_update{idx}.xlsx',
                        sheet_name='toezichtsgroep (Legacy)', index=False, freeze_panes=(1, 1))
                    # Purge dataframe
                    df_assets_lgc.drop(df_assets_lgc.index, inplace=True)
            if not df_assets_lgc.empty:
                df_assets_lgc.to_excel(excel_path / f'map_toezichtsgroep_lgc_{toezichtgroep_bestaand.naam}_update{idx}.xlsx',
                                       sheet_name='toezichtsgroep (Legacy)', index=False, freeze_panes=(1, 1))


    # Write log_df to Excel
    log_df.to_excel(excel_path / "error_log_LGC.xlsx", index=False)
    ExcelModifier(excel_path / "error_log_LGC.xlsx").add_hyperlink(env=Environment.PRD)
    log_df.drop(log_df.index, inplace=True) # purge dataframe

    ######################################
    ### OTL
    ######################################
    # read the complete mapping file (json) in a pandas dataframe
    df_mapping_agents = pd.read_json('mapping_agents.json')

    # loop over de "agents" die fungeren als toezichtsgroep
    for _, agents_map in df_mapping_agents.iterrows():
        agent_bestaand = next(eminfra_client.search_agent(naam=agents_map.toezichtsgroep_existing), None)
        agent_nieuw = next(eminfra_client.search_agent(naam=agents_map.toezichtsgroep_new), None)

        if agent_bestaand and agent_nieuw:
            print(f'Map all assets from agent: {agent_bestaand.naam} to {agent_nieuw.naam}')
            print(f'Processing OTL assets (agent={agent_bestaand.naam} (rol=toezichtsgroep)')
            query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 expansions=ExpansionsDTO(fields=['parent']),
                                 selection=SelectionDTO(
                                     expressions=[ExpressionDTO(
                                         terms=[TermDTO(property='agent', operator=OperatorEnum.EQ,
                                                        value=agent_bestaand.uuid)])]))
            assets = eminfra_client.search_assets(query_dto=query_dto)

            existing_assets = []
            created_assets = []
            for idx, asset in enumerate(assets):
                print(f'\tProcessing asset: {asset.uuid}')
                # Skip asset als het een groepering is.
                if 'grp:installatie' in asset.type.korteUri:
                    log_error(uuid=asset.uuid, message='Skip asset grp:installatie')
                    continue

                if betrokkenerelaties := list(get_bestaande_betrokkenerelaties(asset=asset, rol='toezichtsgroep', agent=agent_bestaand)):
                    [_deactivate_status(item) for item in betrokkenerelaties]

                    # map agent Verkeershandhavingssystemen (EMT_VHS)
                    agent_naam = map_agent(asset=asset) if agent_nieuw.naam == 'EMT_VHS' else agent_nieuw.naam
                    nieuwe_relatie = build_betrokkenerelatie(source=asset, agent_naam=agent_naam, rol='toezichtsgroep')
                    if nieuwe_relatie is None:
                        print(f'Fout bij het aanmaken van een nieuwe relatie voor asset: {asset.uuid}')
                        log_error_missing_locatie(
                            typeURI=betrokkenerelaties[0].typeURI
                            , assetId_identificator=betrokkenerelaties[0].assetId.identificator
                            , bron_typeURI=betrokkenerelaties[0].bron.typeURI
                            , bronAssetId_identificator=betrokkenerelaties[0].bronAssetId.identificator
                            , doel_typeURI=betrokkenerelaties[0].doel.typeURI
                            , doelAssetId_identificator=betrokkenerelaties[0].doelAssetId.identificator
                        )
                        continue
                    nieuwe_relatie.assetId.identificator = f'Hoortbij_{idx}_{asset.type.korteUri}_{agent_nieuw.naam}'

                    # Het deactiveren van bestaande relaties en het toevoegen van nieuwe relaties tesamen uitvoeren.
                    # Op die manier wordt de huidige toezichtsgroep pas gedeactiveerd zodra een nieuwe wordt geactiveerd.
                    existing_assets.extend(betrokkenerelaties)
                    created_assets.append(nieuwe_relatie)

            ######################################
            ### Wegschrijven van de OTL-data
            ######################################
            if existing_assets:
                OtlmowConverter.from_objects_to_file(file_path=excel_path / f'map_toezichtsgroep_otl_{agent_bestaand.naam}_delete_{idx}.xlsx',
                                                     sequence_of_objects=existing_assets)
            if created_assets:
                OtlmowConverter.from_objects_to_file(file_path=excel_path / f'map_toezichtsgroep_otl_{agent_bestaand.naam}_update_{idx}.xlsx',
                                                     sequence_of_objects=created_assets)
    # Save log DataFrame at the end
    log_df.to_excel(excel_path / "error_log_OTL.xlsx", index=False)
    ExcelModifier(excel_path / "error_log_OTL.xlsx").add_hyperlink(env=Environment.PRD)
    print("Log saved to log.xlsx")

    log_df_missing_locatie.to_excel(excel_path / "error_log_OTL_missing_locatie.xlsx", index=False)
    print("Log saved to log.xlsx")