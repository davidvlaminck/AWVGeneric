import json
import logging
from datetime import datetime

from API.EMInfraDomain import KenmerkTypeEnum, BeheerobjectTypeDTO
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

    validate_dataframe_columns(
        df=sheet_df
        , json_path=Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_validatie.json'
        , json_key=sheet_name)

    return sheet_df


def validate_dataframe_columns(df: pd.DataFrame, json_path: Path, json_key: str) -> tuple[list[str], list[str]]:
    """
    Validate that the columns of a DataFrame match the expected columns from a JSON file.

    Parameters:
        df (pd.DataFrame): The DataFrame to validate.
        json_path (str): Path to the JSON file containing expected column definitions.
        json_key (str): The key inside the JSON under which the expected columns are listed.

    Returns:
        Tuple[List[str], List[str]]: A tuple of (missing_columns, extra_columns)
    """
    # Load expected columns from the JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        expected_structure = json.load(f)

    expected_columns = expected_structure["columns"].get(json_key, [])

    if not isinstance(expected_columns, list):
        raise ValueError(f"The value for key '{json_key}' in JSON must be a list.")

    # Compare DataFrame columns
    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)

    missing_columns = list(expected_columns_set - actual_columns)
    extra_columns = list(actual_columns - expected_columns_set)

    if missing_columns or extra_columns:
        logging.error(f"Validation of Excel file failed, sheet: {json_key}")
    if missing_columns:
        logging.error(f"Missing columns: {missing_columns}")
    if extra_columns:
        logging.error(f"Extra columns: {extra_columns}")

    return missing_columns, extra_columns


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Lantis Bypass: \tAanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding')

    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.DEV, auth_type=AuthType.JWT, settings_path=settings_path)

    excel_file = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250410.xlsx'

    df_assets = import_data_as_dataframe(
        filepath=excel_file
        , sheet_name="MIVLVE"
    )

    for idx, asset_row in df_assets.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("'Object naam ROCO")
        asset_row_installatie_naam = asset_row.get("Installatie naam")
        logging.debug(f'Processing asset: {asset_row_uuid}')

        if asset_row_uuid:
            asset = next(eminfra_client.search_asset_by_uuid(asset_row_uuid), None)
            if asset is None:
                logging.debug(f'Asset "{asset_row_uuid}" bestaat nog niet, wordt aangemaakt')
                # Creëer beheerobject indien nodig
                installatie = next(eminfra_client.search_beheerobjecten(naam=asset_row_installatie_naam, actief=True), None)
                if installatie is None:
                    logging.debug(f'Installatie "{asset_row_installatie_naam}" bestaat nog niet, wordt aangemaakt')
                    response_beheerobject = eminfra_client.create_beheerobject(naam=asset_row_installatie_naam)
                    asset_row_installatie_uuid = response_beheerobject.uuid
                else:
                    asset_row_installatie_uuid = installatie.uuid

                # Legacy asset aanmaken
                # todo Legacy asset aangemaakt in de root van de installatie. Later verplaatsen naar de juiste locatie of op de juiste locatie aanmaken
                # todo tot hier
                eminfra_client.create_lgc_asset(parent_uuid=asset_row_installatie_uuid, naam=asset_row_naam, typeUuid=asset_row_typeURI)

        else:
            logging.debug('asset UUID Object ontbreekt, maar asset aan en recupereer de nieuwe UUID')

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
