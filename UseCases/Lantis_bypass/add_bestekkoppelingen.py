import json
import logging
from datetime import datetime
import re

from API.EMInfraDomain import OperatorEnum, AssetDTO
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path



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


def add_bestekkoppeling_if_missing(asset_uuid: str, eDelta_dossiernummer: str, start_datetime: datetime):
    """
    Voeg een specifieke bestekkoppeling toe, indien die nog niet bestaat bij een bepaalde asset.

    :param asset_uuid:
    :param eDelta_dossiernummer:
    :param start_datetime:
    :return:
    """
    huidige_bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid)
    # check if there are currently no bestekkkoppelingen.
    if all(
            bestekkoppeling.bestekRef.eDeltaDossiernummer
            != eDelta_dossiernummer
            for bestekkoppeling in huidige_bestekkoppelingen
    ):
        eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                           start_datetime=datetime(2024, 9, 1))


def zoek_gemigreerde_asset(bron_uuid) -> AssetDTO | None:
    """
    Zoek de OTL-asset op basis van een Legacy-asset die verbonden zijn via een Gemigreerd-relatie.
    Returns None indien de Gemigreerd-relatie ontbreekt.
     
    :param bron_uuid: uuid van de bron asset (Legacy)
    :return: 
    """
    relaties = eminfra_client.search_assetrelaties_OTL(bronAsset_uuid=asset.uuid)
    relatie_gemigreerd = [item for item in relaties if item.get('@type') == 'https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar'][0]
    asset_uuid_gemigreerd = relatie_gemigreerd.get('RelatieObject.doelAssetId').get('DtcIdentificator.identificator')[:36]
    return next(
        eminfra_client.search_asset_by_uuid(asset_uuid=asset_uuid_gemigreerd),
        None,
    )


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Lantis Bypass: \tToevoegen van een bestekkoppeling voor Legacy en OTL assets op basis van de relatie gemigreerd')

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    eDelta_dossiernummer = 'INTERN-095'
    logging.info(f'Bestekkoppeling: {eDelta_dossiernummer}')

    excel_file = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250417_PRD.xlsx'
    logging.info(f"Excel file wordt ingelezen en gevalideerd: {excel_file}")


    df_assets_wegkantkasten = import_data_as_dataframe(filepath=excel_file, sheet_name="Wegkantkasten")

    df_assets_mivlve = import_data_as_dataframe(filepath=excel_file, sheet_name="MIVLVE")

    df_assets_mivmeetpunten = import_data_as_dataframe(filepath=excel_file, sheet_name="MIVMeetpunten")


    # Toevoegen bestekkoppelingen aan de Wegkantkasten
    logging.info('Toevoegen bestekkoppelingen aan Wegkantkasten')
    for idx, asset_row in df_assets_wegkantkasten.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}')

        asset = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset_row_uuid), None)
        if asset is None:
            logging.debug(f'Asset "{asset_row_uuid}" bestaat niet')

        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

    logging.info('Bestekkoppelingen toegevoegd aan Wegkantkasten')


    # Toevoegen bestekkoppelingen aan Meetlussen
    logging.info('Toevoegen bestekkoppelingen aan Meetlussen')
    for idx, asset_row in df_assets_mivlve.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}')

        asset = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset_row_uuid), None)
        if asset is None:
            logging.debug(f'Asset "{asset_row_uuid}" bestaat niet')

        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        verweven_asset = zoek_gemigreerde_asset(bron_uuid=asset.uuid)
        
        add_bestekkoppeling_if_missing(asset_uuid=verweven_asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer, start_datetime=datetime(2024,9,1))

    logging.info('Bestekkoppelingen toegevoegd aan Meetlussen')

    # Toevoegen bestekkoppelingen aan Meetpunten
    logging.info('Toevoegen bestekkoppelingen aan Meetpunten')
    for idx, asset_row in df_assets_mivmeetpunten.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}')

        asset = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset_row_uuid), None)
        if asset is None:
            logging.debug(f'Asset "{asset_row_uuid}" bestaat niet')

        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        verweven_asset = zoek_gemigreerde_asset(bron_uuid=asset.uuid)

        add_bestekkoppeling_if_missing(asset_uuid=verweven_asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

    logging.info('Bestekkoppelingen toegevoegd aan Meetpunten')