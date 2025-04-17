import json
import logging
from datetime import datetime

from API.EMInfraDomain import KenmerkTypeEnum, BeheerobjectTypeDTO, OperatorEnum
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
        logging.info("All validation checks passed")

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


def get_beheerobject_uuid(asset_row_installatie_naam) -> str:
    """
    Geef de uuid terug op basis van de naam van een installatie.
    Maak het beheerobject aan indien het nog niet bestaat

    :param asset_row_installatie_naam:
    :return: uuid van een beheerobject (installatie)
    """
    installatie = next(eminfra_client.search_beheerobjecten(naam=asset_row_installatie_naam, actief=True, operator=OperatorEnum.EQ), None)
    if installatie is None:
        logging.info(f'Installatie "{asset_row_installatie_naam}" bestaat nog niet, wordt aangemaakt')
        response_beheerobject = eminfra_client.create_beheerobject(naam=asset_row_installatie_naam)
        asset_row_installatie_uuid = response_beheerobject.uuid
    else:
        asset_row_installatie_uuid = installatie.uuid
    logging.info(f'Installatie uuid: {asset_row_installatie_uuid}')
    return asset_row_installatie_uuid


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Lantis Bypass: \tAanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=settings_path)

    excel_file = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250417.xlsx'

    df_assets = import_data_as_dataframe(
        filepath=excel_file
        , sheet_name="Wegkantkasten"
    )

    for idx, asset_row in df_assets.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("Object naam ROCO")
        asset_row_installatie_naam = asset_row.get("Installatie naam")
        asset_row_installatie_uuid = get_beheerobject_uuid(asset_row_installatie_naam)

        logging.debug(f'Processing asset: {asset_row_uuid}')

        if asset_row_uuid:
            logging.debug('Controle op het feit dat voor een bepaalde uuid, er al een asset bestaat.')
            asset = next(eminfra_client.search_asset_by_uuid(asset_row_uuid), None)
            if asset is None:
                logging.critical(f'Asset {asset_row_uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')
                # raise ValueError(f'Asset {asset_row_uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')
        else:
            logging.debug(f'Asset met de naam "{asset_row_naam}" ontbreekt')
            assettype_uuid = [item.uuid for item in eminfra_client.get_all_assettypes() if item.uri == f'{asset_row_typeURI}'][0]
            # todo Legacy asset aangemaakt in de root van de installatie. Later verplaatsen naar de juiste locatie of op de juiste locatie aanmaken
            # Test: kan men dit aanmaken in de root? Zoniet, een andere methode implementeren om een asset aan te maken, en dan nadien op de juiste plaats in de boom hangen.
            asset_dict = eminfra_client.create_lgc_asset(parent_uuid=asset_row_installatie_uuid, naam=asset_row_naam, typeUuid=assettype_uuid)
            logging.debug(f'Asset met de naam "{asset_row_naam}" is aangemaakt en heeft uuid: {asset.uuid}')


        # Indien de asset-eigenschappen niet bestaan >> aanmaken
        # Indien de asset-relaties niet bestaan >> aanmaken


    #     kenmerken = eminfra_client.get_kenmerken(assetId=asset.get("uuid_PT-verwerkingseenheid"))
    #     kenmerk_bevestiging = eminfra_client.get_kenmerken(assetId=asset.get("uuid_PT-verwerkingseenheid"),
    #                                                        naam=KenmerkTypeEnum.BEVESTIGD_AAN)
    #
    #     # Query asset
    #     relatieTypeId = '3ff9bf1c-d852-442e-a044-6200fe064b20'
    #     bestaande_relaties = eminfra_client.search_relaties(
    #         assetId=asset.get("uuid_PT-verwerkingseenheid")
    #         , kenmerkTypeId=kenmerk_bevestiging.type.get("uuid")
    #         , relatieTypeId=relatieTypeId
    #     )
    #
    #     # Query asset-relaties. Als de relatie al bestaat, continue
    #     if next(bestaande_relaties, None):
    #         print(
    #             f'''Bevestiging-relatie reeds bestaande tussen PT-Verwerkingseenheid ({asset.get(
    #                 "uuid_PT-verwerkingseenheid")}) en PT-Demodulator ({asset.get("uuid_PT-demodulator")})''')
    #         continue
    #
    #     # Genereer relatie volgens het OTLMOW-model
    #     # todo vervang parameter target_typeURI door de juiste URI. Momenteel tijdelijk assettype PTRegelaar gebruikt ter afwachting van de release van de nieuwe OTL.
    #     nieuwe_relatie = create_relation(
    #         relation_type=Bevestiging()
    #         , source_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTVerwerkingseenheid'
    #         , source_uuid=asset.get("uuid_PT-verwerkingseenheid")
    #         , target_typeURI='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTRegelaar'
    #         # 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#PTDemodulatoren'
    #         , target_uuid=asset.get("uuid_PT-demodulator")
    #     )
    #     nieuwe_relatie.isActief = True
    #     nieuwe_relatie.assetId.identificator = f'Bevestiging_{asset.get("uuid_PT-verwerkingseenheid")}_{asset.get("uuid_PT-demodulator")}'
    #     bevestiging_relaties.append(nieuwe_relatie)
    #
    # ######################################
    # ### Wegschrijven van de OTL-data naar een DAVIE-conform bestand.
    # ######################################
    # if bevestiging_relaties:
    #     filepath = Path().home() / 'Downloads' / 'Assetrelaties' / f'BevestigingRelatie_PT-verwerkingseenheid_PT-demodulator_{datetime.now().strftime("%Y%m%d")}.xlsx'
    #     OtlmowConverter.from_objects_to_file(
    #         file_path=filepath
    #         , sequence_of_objects=bevestiging_relaties
    #     )
    #     print(f"DAVIE-file weggeschreven naar:\n\t{filepath}")
