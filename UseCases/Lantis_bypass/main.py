import json
import logging
from datetime import datetime
import re

from API.EMInfraDomain import KenmerkTypeEnum, BeheerobjectTypeDTO, OperatorEnum, BoomstructuurAssetTypeEnum, \
    AssetDTOToestand, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, TermDTO, ExpressionDTO, LogicalOpEnum
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation, create_relation
from otlmow_converter.OtlmowConverter import OtlmowConverter
from otlmow_model.OtlmowModel.Classes.Onderdeel.Bevestiging import Bevestiging

def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'


def validate_not_null_column(df: pd.DataFrame, column: str):
    pass


def import_data_as_dataframe(filepath: Path, sheet_name: str = None):
    """Import data as a dataframe

    Read input data Componententlijst into a DataFrame. Validate the data structure.
    """
    # Read the Excel file
    sheet_df = pd.read_excel(
        filepath,
        header=1,  # skip the first row and set the second row as headers
        sheet_name=sheet_name
    )
    # drop the first row of the dataframe "in te vullen door: ... and the first columns of the dataframe
    sheet_df = sheet_df.drop(index=sheet_df.index[0], columns=sheet_df.columns[0])

    sheet_df.drop(columns=[col for col in sheet_df.columns if col.startswith('Comments')], inplace=True)
    sheet_df.drop(columns=[col for col in sheet_df.columns if col.startswith('Unnamed')], inplace=True)

    # convert NaN to None
    sheet_df = sheet_df.where(pd.notna(sheet_df), None)

    validation_results = validate_dataframe_columns(
        df=sheet_df
        , schema_path=Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_validatie.json'
        , schema_key=sheet_name)

    if any(validation_results.values()):
        logging.critical("Validation errors found:")
        for k, v in validation_results.items():
            if v:
                logging.error(f"{k}: {v}")

        # Raise if errors exist
        raise ValueError("Validation of DataFrame structure failed. See logs for details.")
    else:
        logging.info(f"All validation checks passed for sheet: {sheet_name}")

    return sheet_df


def validate_dataframe_columns(df: pd.DataFrame, schema_path: Path, schema_key: str) -> tuple[list[str], list[str]]:
    """
    Validate that the columns of a DataFrame match the expected columns from a JSON file.

    Parameters:
        df (pd.DataFrame): The DataFrame to validate.
        schema_path (str): Path to the JSON file containing expected column definitions.
        schema_key (str): The key inside the JSON under which the expected columns are listed.

    Returns:
        Tuple[List[str], List[str]]: A tuple of (missing_columns, extra_columns)
    """
    # Load expected columns from the JSON file
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)[schema_key]

    expected_columns = [col['name'] for col in schema]
    actual_columns = df.columns.tolist()

    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]

    type_errors = []
    nullability_errors = []

    for col_def in schema:
        col_name = col_def['name']
        expected_type = col_def.get('type')
        nullable = col_def.get('nullable', True)

        if col_name in df.columns:
            actual_type = str(df[col_name].dtype)
            if expected_type and actual_type != expected_type:
                type_errors.append(f"Column '{col_name}' expected type '{expected_type}', got '{actual_type}'")

            if not nullable and df[col_name].isnull().any():
                nullability_errors.append(f"Column '{col_name}' should not contain nulls")

    return {
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_errors": type_errors,
        "nullability_errors": nullability_errors
    }


def create_installatie(naam: str) -> str:
    """
    Maak de installatie (beheerobject) aan indien onbestaande

    :param naam: naam van de installatie
    :return: uuid van de installatie
    """
    installatie = next(eminfra_client.search_beheerobjecten(naam=naam, actief=True, operator=OperatorEnum.EQ), None)
    if installatie is None:
        logging.info(f'Installatie "{naam}" bestaat nog niet, wordt aangemaakt')
        response_beheerobject = eminfra_client.create_beheerobject(naam=naam)
        asset_row_installatie_uuid = response_beheerobject.get("uuid")
    else:
        asset_row_installatie_uuid = installatie.uuid
    logging.info(f'Installatie uuid: {asset_row_installatie_uuid}')
    return asset_row_installatie_uuid


def get_assettype_uuid(mapping_key: str) -> str:
    """
    Returns the assettype uuid of a Legacy assettype. The keys of the mapping file correspond with the names in the Excel-file.
    :param mapping_key:
    :return:
    """
    mapping_assettypes = {
        "Wegkantkasten": "10377658-776f-4c21-a294-6c740b9f655e",
        "HSCabines-CC-SC-HS-LS-Switch-WV": "1cf24e76-5bf3-44b0-8332-a47ab126b87e",
        "Openbare verlichting": "4dfad588-277c-480f-8cdc-0889cfaf9c78",
        "MIVLVE": "a4c75282-972a-4132-ad72-0d0de09dbdb8",
        "MIVMeetpunten": "dc3db3b7-7aad-4d7f-a788-a4978f803021",
        "RSS-borden": "1496b2fd-0742-44a9-a3b4-e994bd5af8af",
        "(R)VMS-borden": "5b44cb96-3edf-4ef5-bc85-ec4d5c5152a3",
        "Cameras": "f66d1ad1-4247-4d99-80bb-5a2e6331eb96",
        "Portieken-Seinbruggen": "",
        "Galgpaal": ""
    }
    return mapping_assettypes[mapping_key]


def construct_installatie_naam(kastnaam: str) -> str:
    """
    Bouw de installatie naam op basis van de kastnaam.
    Verwijder suffix ".K".
    Hernoem letter P/N/M door X. Deze letter duidt de rijrichting aan (Positief, Negatief, Middenberm) en volgt net na de naam van de rijweg.
    Voorbeeld:
    kastnaam: A13M0.5.K
    installatie_naam: A13X0.5

    :param kastnaam:
    :return: installatie_naam
    """
    # Step 1: Remove suffix ".K" if present
    if kastnaam.endswith('.K'):
        temp_installatie_naam = kastnaam[:-2]
    else:
        raise ValueError(f"Kastnaam {kastnaam} eindigt niet op '.K'")

    if match := re.search(r'(.*)([MPN])(?!.*[MPN])', temp_installatie_naam):
        installatie_naam = match.group(1) + 'X' + temp_installatie_naam[match.end():]
    else:
        raise ValueError("De syntax van de kast bevat geen letter 'P', 'N' of 'M'.")

    return installatie_naam


def validate_asset(uuid: str = None, naam: str = None, stop_on_error: bool = True) -> None:
    """
    Controleer het bestaan van een asset op basis van diens uuid.
    Valideer nadien of de niuewe naam overeenstemt met de naam van de bestaande asset.

    :param uuid: asset uuid
    :param naam: asset name
    :param stop_on_error: Raise Error (default True)
    :type stop_on_error: boolean
    :return: None
    """
    logging.debug('Valideer of een asset reeds bestaat en of diens naam overeenkomt.')
    asset = next(eminfra_client.search_asset_by_uuid(uuid), None)

    if asset is None:
        logging.error(f'Asset {uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')
        if stop_on_error:
            raise ValueError(f'Asset {asset_row_uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')

    if naam != asset.naam:
        logging.error(
            f'Asset {uuid} naam {naam} komt niet overeen met de bestaande naam {asset.naam}.')
        if stop_on_error:
            raise ValueError(
                f'Asset {asset_row_uuid} naam {asset_row_naam} komt niet overeen met de bestaande naam {asset.naam}.')
    return None


def parse_wkt_geometry(asset_row):
    asset_row_x = asset_row.get('Positie X (Lambert 72)')
    asset_row_y = asset_row.get('Positie Y (Lambert 72)')
    asset_row_z = asset_row.get('Positie Z (Lambert 72, optioneel)')
    if asset_row_z is None:
        asset_row_z = 0
    return f'POINT Z ({asset_row_x} {asset_row_y} {asset_row_z})'


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Lantis Bypass: \tAanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding')

    settings_path = load_settings()
    environment = Environment.DEV
    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)
    logging.info(f'Omgeving: {environment.name}')

    excel_file = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250417_DEV.xlsx'
    logging.info(f"Excel file wordt ingelezen en gevalideerd: {excel_file}")
    df_assets_wegkantkasten = import_data_as_dataframe(
        filepath=excel_file
        , sheet_name="Wegkantkasten"
    )

    df_assets_mivlve = import_data_as_dataframe(
        filepath=excel_file
        , sheet_name="MIVLVE"
    )

    df_assets_mivmeetpunten = import_data_as_dataframe(
        filepath=excel_file
        , sheet_name="MIVMeetpunten"
    )

    # Aanmaken van de Installaties (beheerobject)
    logging.info('Aanmaken van installaties op basis van de kastnamen')
    installaties = []
    for idx, asset_row in df_assets_wegkantkasten.iterrows():
        asset_row_naam = asset_row.get("Object assetId.identificator")
        installatie_naam = construct_installatie_naam(kastnaam=asset_row_naam)
        installaties.append(installatie_naam)
        asset_row_installatie_uuid = create_installatie(naam=installatie_naam)
    logging.info(f'Installaties aangemaakt: {installaties}')


    # Aanmaken van de Wegkantkasten
    logging.info('Aanmaken van Wegkantkasten onder installaties')
    wegkantkasten = []
    for idx, asset_row in df_assets_wegkantkasten.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("Object assetId.identificator")
        assettype_uuid = get_assettype_uuid(mapping_key='Wegkantkasten')
        installatie_naam = construct_installatie_naam(kastnaam=asset_row_naam)
        installatie = next(eminfra_client.search_beheerobjecten(naam=installatie_naam, actief=True, operator=OperatorEnum.EQ))

        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_naam}')

        if asset_row_uuid and asset_row_naam:
            logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
            validate_asset(uuid=asset_row_uuid, naam=asset_row_naam, stop_on_error=True)

        asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_naam, exact_search=True), None)
        if asset is None:
            logging.debug(f'Asset met als naam "{asset_row_naam}" bestaat niet en wordt aangemaakt')
            asset_dict = eminfra_client.create_asset(
                parent_uuid=installatie.uuid
                , naam=asset_row_naam
                , typeUuid=assettype_uuid
                , parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT
            )
            asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_naam, exact_search=True), None)

        if asset is None:
            logging.critical('Asset werd niet aangemaakt')

        # Update toestand
        if asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW}"')
            eminfra_client.update_toestand(asset_uuid=asset.uuid, asset_naam=asset.naam, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        asset_locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)
        if asset_locatiekenmerk.geometrie is None:
            asset_row_wkt_geometry = parse_wkt_geometry(asset_row = asset_row)
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        wegkantkasten.append(f'{asset.uuid}: {asset.naam}')

    logging.info(f'Wegkantkasten aangemaakt: {wegkantkasten}')


    # Aanmaken van de MIVLVE
    logging.info('Aanmaken van MIVLVE onder Wegkantkasten')
    mivlve = []
    for idx, asset_row in df_assets_mivlve.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("Object assetId.identificator")
        assettype_uuid = get_assettype_uuid(mapping_key='MIVLVE')
        asset_row_parent_name = asset_row.get("Bevestigingsrelatie doelAssetId.identificator")
        asset_row_parent_asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)
        if asset_row_parent_asset is None:
            logging.critical(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVLVE "{asset_row_uuid}" en diens bevestiging-relatie')
            raise ValueError(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVLVE "{asset_row_uuid}" en diens bevestiging-relatie')
        asset_row_parent_asset_uuid = asset_row_parent_asset.uuid
        asset_row_parent_asset_name = asset_row_parent_asset.naam
        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_naam}')

        if asset_row_uuid and asset_row_naam:
            logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
            validate_asset(uuid=asset_row_uuid, naam=asset_row_naam, stop_on_error=True)

        asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_naam, exact_search=True), None)
        if asset is None:
            logging.debug(f'Asset met als naam "{asset_row_naam}" bestaat niet en wordt aangemaakt')
            asset_dict = eminfra_client.create_asset(
                parent_uuid=asset_row_parent_asset_uuid
                , naam=asset_row_naam
                , typeUuid=assettype_uuid
                , parent_asset_type=BoomstructuurAssetTypeEnum.ASSET
            )
            asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_naam, exact_search=True), None)

        if asset is None:
            logging.critical('Asset werd niet aangemaakt')

        # Update toestand
        if asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW}"')
            eminfra_client.update_toestand(asset_uuid=asset.uuid, asset_naam=asset.naam, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        asset_locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)
        if asset_locatiekenmerk.geometrie is None:
            asset_row_wkt_geometry = parse_wkt_geometry(asset_row = asset_row)
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # todo tot hier (eigenschappen)
        # update eigenschap XXX

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        mivlve.append(f'{asset.uuid}: {asset.naam}')
    logging.info('MIVLVE aangemaakt')

    # Aanmaken van de MIVMeetpunten
    logging.info('Aanmaken van MIVMeetpunten onder MIVLVE')
    mivmeetpunten = []
    for idx, asset_row in df_assets_mivmeetpunten.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("Object assetId.identificator")
        assettype_uuid = get_assettype_uuid(mapping_key='MIVMeetpunten')
        assettype_uuid_parent = get_assettype_uuid(mapping_key='MIVLVE')
        asset_row_parent_name = asset_row.get("Sturingsrelatie bron AssetId.identificator")
        query_search_parent = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(expressions=[
                    ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=f'{assettype_uuid_parent}')]),
                    ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=f'{asset_row_parent_name}')], logicalOp=LogicalOpEnum.AND)
            ]))
        asset_row_parent_asset = next(eminfra_client.search_assets(query_dto=query_search_parent), None)
        if asset_row_parent_asset is None:
            logging.critical(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVMeetpunt "{asset_row_uuid}" en diens Sturing-relatie')
            raise ValueError(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVMeetpunt "{asset_row_uuid}" en diens Sturing-relatie')
        asset_row_parent_asset_uuid = asset_row_parent_asset.uuid
        asset_row_parent_asset_name = asset_row_parent_asset.naam
        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_naam}')

        if asset_row_uuid and asset_row_naam:
            logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
            validate_asset(uuid=asset_row_uuid, naam=asset_row_naam, stop_on_error=True)

        query = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(expressions=[
                    ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=f'{assettype_uuid}')]),
                    ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=f'{asset_row_naam}')], logicalOp=LogicalOpEnum.AND),
                    ExpressionDTO(terms=[TermDTO(property='naampad', operator=OperatorEnum.CONTAINS, value=f'{asset_row_parent_asset_name}')], logicalOp=LogicalOpEnum.AND)
            ]))
        # todo tot hier. Debug onderstaande functie
        asset = next(eminfra_client.search_assets(query_dto=query), None)
        if asset is None:
            logging.debug(f'Asset met als naam "{asset_row_naam}" bestaat niet en wordt aangemaakt')
            asset_dict = eminfra_client.create_asset(
                parent_uuid=asset_row_parent_asset_uuid
                , naam=asset_row_naam
                , typeUuid=assettype_uuid
                , parent_asset_type=BoomstructuurAssetTypeEnum.ASSET
            )
            query = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             expansions=ExpansionsDTO(fields=['parent'])
                             , selection=SelectionDTO(expressions=[
                    ExpressionDTO(
                        terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=f'{assettype_uuid}')]),
                    ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=f'{asset_row_naam}')],
                                  logicalOp=LogicalOpEnum.AND),
                    ExpressionDTO(terms=[TermDTO(property='naampad', operator=OperatorEnum.CONTAINS,
                                                 value=f'{asset_row_parent_asset_name}')], logicalOp=LogicalOpEnum.AND)
                ]))
            asset = next(eminfra_client.search_assets(query_dto=query), None)

        if asset is None:
            logging.critical('Asset werd niet aangemaakt')

        # Update toestand
        if asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW}"')
            eminfra_client.update_toestand(asset_uuid=asset.uuid, asset_naam=asset.naam, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        asset_locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid)
        if asset_locatiekenmerk.geometrie is None:
            asset_row_wkt_geometry = parse_wkt_geometry(asset_row = asset_row)
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # todo tot hier (eigenschappen)
        # update eigenschap XXX

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        mivmeetpunten.append(f'{asset.uuid}: {asset.naam}')
    logging.info('MIVMeetpunten aangemaakt')

    # Aanmaken of updaten van de eigenschappen
    # Aanmaken of updaten van de relaties
