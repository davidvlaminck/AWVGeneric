from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Classes.Onderdeel import Beschermbuis, Voedingskabel, Signaalkabel
from otlmow_model.OtlmowModel.BaseClasses.OTLObject import dynamic_create_instance_from_uri
from otlmow_model.OtlmowModel.Classes.Onderdeel.HeeftBetrokkene import HeeftBetrokkene
from otlmow_model.OtlmowModel.Classes.ImplementatieElement.RelatieObject import RelatieObject
from otlmow_model.OtlmowModel.Helpers.RelationCreator import Agent, create_betrokkenerelation, create_relation
from otlmow_model.OtlmowModel.BaseClasses.MetaInfo import meta_info
from otlmow_model.OtlmowModel.Datatypes.KlBetrokkenheidRol import KlBetrokkenheidRol
from otlmow_converter.OtlmowConverter import OtlmowConverter


def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'


def read_report(downloads_subpath: str, sheet_name: str = 'Resultaat', usecols: list = ["uuid"]):
    """Read RSA-report as input into a DataFrame."""
    filepath = Path().home() / 'Downloads' / downloads_subpath
    df_assets = pd.read_excel(filepath, sheet_name=sheet_name, header=2, usecols=usecols)
    df_assets = df_assets.where(pd.notna(df_assets), None)
    df_assets.drop_duplicates(inplace=True)
    return df_assets


def construct_full_name(first_name: str, last_name: str) -> str | None:
    return " ".join([first_name, last_name]) if first_name and last_name else None


if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = read_report(
        downloads_subpath='toezichter/[RSA] Bijhorende assets hebben een verschillende toezichtshouder (assettype = Beschermbuis).xlsx',
        usecols=["otl_uuid", "otl_uri", "lgc_uuid", "lgc_toezichthouder_gebruikersnaam", "lgc_toezichtsgroep_naam",
                 "lgc_toezichthouder_voornaam", "lgc_toezichthouder_naam"])

    created_assets = []
    for index, asset in df_assets.iloc[15:16].iterrows():  # todo remove slicing of the dataframe
        # otl_object = dynamic_create_instance_from_uri(asset.otl_uri)
        # print(meta_info(otl_object))

        # search otl_asset and extract the uuid of the asset. This ensures that the asset exists.
        otl_asset = eminfra_client.search_asset_by_uuid(asset_uuid=asset.otl_uuid)

        # search toezichter and extract the uuid of the toezichter. This ensures that the toezichter exists.
        toezichthouder = construct_full_name(first_name=asset.lgc_toezichthouder_voornaam,
                                             last_name=asset.lgc_toezichthouder_naam)
        if toezichthouder:
            generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='agents',
                                                                                    filter_string={
                                                                                        "naam": toezichthouder})
            agents = list(generator_agents)
            if len(agents) != 1:
                print('Agent was not found or returned multiple results.')
                continue
            agent_uuid = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]
            agent_uri = agents[0].get('@type')

            relatie_toezichter = create_betrokkenerelation(rol='toezichter', source_typeURI=asset.otl_uri,
                                                           source_uuid=asset.otl_uuid, target_uuid=agent_uuid,
                                                           target_typeURI=agent_uri)
            relatie_toezichter.assetId.identificator = f'HeeftBetrokkene_{index}'

            created_assets.append(relatie_toezichter)

        toezichtgroep = asset.lgc_toezichtsgroep_naam
        if toezichtgroep:
            generator_toezichtgroep = eminfra_client.search_toezichtgroep(naam=toezichtgroep)
            toezichtgroepen = list(generator_toezichtgroep)
            if len(toezichtgroepen) != 1:
                print('Toezichtgroep was not found or returned multiple results.')
                continue
            toezichtsgroep_uuid = toezichtgroepen[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[
                                  :36]
            toezichtsgroep_uri = toezichtgroepen[0].get('@type')

            relatie_toezichtsgroep = create_betrokkenerelation(rol='toezichtsgroep', source_typeURI=asset.otl_uri,
                                                               source_uuid=asset.otl_uuid,
                                                               target_uuid=toezichtsgroep_uuid,
                                                               target_typeURI=toezichtsgroep_uri)
            relatie_toezichtsgroep.assetId.identificator = f'HeeftBetrokkene_{index}'

            created_assets.append(relatie_toezichtsgroep)
        # todo: nakijken om de uuid 36 karakters bevat of meer.
        # search toezichtsgroep and extract the uuid of the toezichtsgroep. This ensures that the toezichtsgroep exists.
        # otl_object.assetId.identificator = asset.otl_uuid

        # create relatie, sla die relatie ook op in de pool (lijst) van alle assets.
        # relatie_toezichter = create_betrokkenerelation(rol='toezichter', source_uuid=asset.otl_uuid, target_uuid=agent_uuid)
        # relatie_toezichtsgroep = create_betrokkenerelation(rol='toezichtsgroep', source_uuid=asset.otl_uuid, target_uuid=agent_uuid)
        #
        # created_assets.append(relatie_toezichter)
        # created_assets.append(relatie_toezichtsgroep)

    OtlmowConverter.from_objects_to_file(file_path=Path('assets_update_toezichter_toezichtsgroep.xlsx'),
                                         sequence_of_objects=created_assets)

    #
    #
    #
    #
    # asset_uuid_otl = asset['otl_uuid']
    # asset_uuid_lgc = asset['lgc_uuid']
    # lgc_toezichthouder_full_name = f'{asset["lgc_toezichthouder_voornaam"]} {asset["lgc_toezichthouder_naam"]}'
    # print(f"Updating HeeftBetrokkene-relatie Toezichter for asset: {asset_uuid_otl}")
    #
    # #################################################################################
    # ####  Get betrokkenerelatie from OTL-asset (rol=toezichter)
    # #################################################################################
    # generator_betrokkenerelaties = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='betrokkenerelaties', filter_string={"bronAsset": asset_uuid_otl, 'rol': 'toezichter'})
    # betrokkenerelaties = list(generator_betrokkenerelaties)
    # if len(betrokkenerelaties) != 1:
    #     print(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid_otl}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')
    #     continue
    #     # raise ValueError(f'Exactly 1 betrokkenerelaties (type: toezichter) are expected for asset: {asset_uuid_otl}.\nFound {len(betrokkenerelaties)} betrokkenerelaties')
    # agent_uuid_otl = betrokkenerelaties[0].get('RelatieObject.doelAssetId').get('DtcIdentificator.identificator')[:36]   # agent_uuid (de persoon)
    # betrokkenerelatie_uuid_otl = betrokkenerelaties[0].get('RelatieObject.assetId').get('DtcIdentificator.identificator')[:36]  # betrokkenerelatie_uuid (het relatieobject tussen een asset en een persoon)
    #
    # #################################################################################
    # ####  Get agent from the LGC-asset
    # #################################################################################
    # generator_agents = eminfra_client.get_objects_from_oslo_search_endpoint(size=1, url_part='agents', filter_string={"naam": lgc_toezichthouder_full_name})
    # agents = list(generator_agents)
    # if len(agents) != 1:
    #     print(f'Agent {lgc_toezichthouder_full_name} was not found or returned multiple results.')
    #     continue
    #     # raise ValueError(f'Agent {lgc_toezichthouder_full_name} was not found or returned multiple results.')
    # agent_uuid_lgc = agents[0].get('purl:Agent.agentId').get('DtcIdentificator.identificator')[:36]
    #
    # #################################################################################
    # ####  Add a new betrokkenerelatie - type: toezichter - to OTL-asset
    # #################################################################################
    # response = eminfra_client.add_betrokkenerelatie(asset_uuid=asset_uuid_otl, agent_uuid=agent_uuid_lgc,
    #                                                 rol='toezichter')
    # betrokkenerelatie_uuid_otl_new = response.get('uuid')
    #
    # #################################################################################
    # ####  Remove betrokkenerelatie toezichter from OTL-asset
    # #################################################################################
    # response = eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid_otl)
    #
