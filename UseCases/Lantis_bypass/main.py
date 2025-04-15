import json
from datetime import datetime
from API.EMInfraDomain import KenmerkTypeEnum
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from otlmow_model.OtlmowModel.Helpers.RelationCreator import create_betrokkenerelation, create_relation
from otlmow_converter.OtlmowConverter import OtlmowConverter
from otlmow_model.OtlmowModel.Classes.Onderdeel.Bevestiging import Bevestiging

print(
    """"
    Lantis Bypass
    Aanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding
    """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path


def import_data_as_dataframe(filepath: Path, sheet_name: [str] = None):
    """Import data as a dataframe

    Read input data Componententlijst into a DataFrame. Validate the data structure.
    """
    # Read the Excel file
    sheet_dfs = pd.read_excel(
        filepath,
        header=None,  # No header yet
        skiprows=[0],  # Skip the first row (index 0)
        usecols=lambda x: x != 'A',  # Skip column A (first column)
        sheet_name=sheet_name
    )
    # Now, the first row in sheet_dfs (index 0) is the "second row" from the original file
    # Set the first row as header
    sheet_dfs.columns = sheet_dfs.iloc[0]
    # Drop the "header" row and the next (third) row
    sheet_dfs = sheet_dfs.drop(index=[0, 1])
    # Reset the index
    sheet_dfs = sheet_dfs.reset_index(drop=True)

    # Load validation info
    expected_sheets, expected_columns = load_expected_structure(json_path= Path(__file__).resolve().parent() / 'data' / 'input' / 'Componentenlijst_validatie.json')

    # validate the Excel structure
    validate_excel_structure(sheet_dfs=sheet_dfs, expected_sheets=expected_sheets)

    return sheet_dfs


class ExcelValidationError(Exception):
    """Custom exception to capture Excel structure validation issues."""
    pass


def load_expected_structure(json_path: str) -> tuple[list[str], dict[str, list[str]]]:
    """Load the expected sheets and columns from a JSON file."""
    with open(json_path, 'r') as f:
        structure = json.load(f)
    expected_sheets = structure["sheets"]
    expected_columns = structure["columns"]
    return expected_sheets, expected_columns


def validate_excel_structure(sheet_dfs: dict[str, pd.DataFrame], expected_sheets: dict[str, list[str]]) -> None:
    """
    Validate the structure of an Excel file based on expected sheet names and columns.

    Parameters:
    - sheet_dfs: dictionary where keys = sheet names, values = DataFrames
    - expected_sheets: dictionary where keys = expected sheet names, values = expected column names
    """
    errors = []

    # Check if the sheet names match
    actual_sheets = set(sheet_dfs.keys())
    expected_sheet_names = set(expected_sheets.keys())

    missing_sheets = expected_sheet_names - actual_sheets
    extra_sheets = actual_sheets - expected_sheet_names

    if missing_sheets:
        errors.append(f"Missing sheets: {', '.join(missing_sheets)}")
    if extra_sheets:
        errors.append(f"Unexpected sheets: {', '.join(extra_sheets)}")

    # Validate columns in each sheet
    for sheet_name, expected_columns in expected_sheets.items():
        df = sheet_dfs.get(sheet_name)
        if df is None:
            continue  # Already reported missing sheet
        sheet_errors = validate_sheet_columns(df, sheet_name, expected_columns)
        errors.extend(sheet_errors)

    if errors:
        error_message = "Excel structure validation failed:\n" + "\n".join(errors)
        raise ValueError(error_message)


def validate_sheet_columns(df: pd.DataFrame, sheet_name: str, expected_columns: list[str]) -> list[str]:
    """
    Validate the columns of a single sheet (DataFrame).

    Parameters:
    - df: the DataFrame for the sheet
    - sheet_name: name of the sheet
    - expected_columns: list of expected column names

    Returns:
    - List of error messages
    """
    errors = []

    actual_columns = list(df.columns)
    if actual_columns != expected_columns:
        errors.append(
            f"Sheet '{sheet_name}' has unexpected columns.\n"
            f"Expected: {expected_columns}\n"
            f"Found: {actual_columns}"
        )

    return errors




if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=settings_path)

    # todo: opsplitsen validatie en inlezen in 2 aparte stappen. Eerst de validatie van alle sheets, nadien het inlezen van een specifieke sheet.
    
    # Read input data
    df_asset_MIVLVE = import_data_as_dataframe(
        filepath = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250410.xlsx'
        , sheet_name=["MIVLVE"]
    )

    bevestiging_relaties = []
    for idx, asset in df_assets.iterrows():
        # Get kenmerken
        kenmerken = eminfra_client.get_kenmerken(assetId=asset.get("uuid_PT-verwerkingseenheid"))
        kenmerk_bevestiging = eminfra_client.get_kenmerken(assetId=asset.get("uuid_PT-verwerkingseenheid"),
                                                           naam=KenmerkTypeEnum.BEVESTIGD_AAN)

        # Query asset
        relatieTypeId = '3ff9bf1c-d852-442e-a044-6200fe064b20'
        bestaande_relaties = eminfra_client.search_relaties(
            assetId=asset.get("uuid_PT-verwerkingseenheid")
            , kenmerkTypeId=kenmerk_bevestiging.type.get("uuid")
            , relatieTypeId=relatieTypeId
        )

        # Query asset-relaties. Als de relatie al bestaat, continue
        if next(bestaande_relaties, None):
            print(
                f'''Bevestiging-relatie reeds bestaande tussen PT-Verwerkingseenheid ({asset.get(
                    "uuid_PT-verwerkingseenheid")}) en PT-Demodulator ({asset.get("uuid_PT-demodulator")})''')
            continue

        # Genereer relatie volgens het OTLMOW-model
        # todo vervang parameter target_typeURI door de juiste URI. Momenteel tijdelijk assettype PTRegelaar gebruikt ter afwachting van de release van de nieuwe OTL.
        nieuwe_relatie = create_relation(
            relation_type=Bevestiging()
            , source_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTVerwerkingseenheid'
            , source_uuid=asset.get("uuid_PT-verwerkingseenheid")
            , target_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTRegelaar'
            # 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTDemodulatoren'
            , target_uuid=asset.get("uuid_PT-demodulator")
        )
        nieuwe_relatie.isActief = True
        nieuwe_relatie.assetId.identificator = f'Bevestiging_{asset.get("uuid_PT-verwerkingseenheid")}_{asset.get("uuid_PT-demodulator")}'
        bevestiging_relaties.append(nieuwe_relatie)

    ######################################
    ### Wegschrijven van de OTL-data naar een DAVIE-conform bestand.
    ######################################
    if bevestiging_relaties:
        filepath = Path().home() / 'Downloads' / 'Assetrelaties' / f'BevestigingRelatie_PT-verwerkingseenheid_PT-demodulator_{datetime.now().strftime("%Y%m%d")}.xlsx'
        OtlmowConverter.from_objects_to_file(
            file_path= filepath
            , sequence_of_objects=bevestiging_relaties
        )
        print(f"DAVIE-file weggeschreven naar:\n\t{filepath}")
