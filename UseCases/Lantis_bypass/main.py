import json
import logging
from datetime import datetime
import re

from API.EMInfraDomain import OperatorEnum, BoomstructuurAssetTypeEnum, \
    AssetDTOToestand, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, TermDTO, ExpressionDTO, LogicalOpEnum, \
    AssetDTO
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
         header=[0,1],  # skip the first row and set the second row as headers
        sheet_name=sheet_name
    )
    # Combine multi-level columns into a single string. Concatenate row1 and row2 into one column
    sheet_df.columns = [f'{col[0]}_{col[1]}' for col in sheet_df.columns]

    # drop the first row of the dataframe "in te vullen door: ... and the first columns of the dataframe
    sheet_df = sheet_df.drop(index=sheet_df.index[0], columns=sheet_df.columns[0])

    sheet_df.drop(columns=[col for col in sheet_df.columns if 'Comments' in col], inplace=True)

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


def create_installatie_if_missing(naam: str) -> str:
    """
    Maak de installatie (beheerobject) aan indien onbestaande en geef de uuid terug.

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

def create_asset_if_missing(typeURI: str, asset_naam: str, parent_uuid: str, parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT) -> AssetDTO:
    """
    Maak de asset aan indien nog onbestaande en geef de asset terug

    :param typeURI: asset typeURI
    :asset_naam: asset naam
    :parent_uuid: parent uuid
    :parent_asset_type:
    :return: asset
    """
    assettype_uuid = get_assettype_uuid(assettype_URI=typeURI)
    query_dto = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent'])
                         , selection=SelectionDTO(expressions=[
                ExpressionDTO(terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=f'{assettype_uuid}')]),
                ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.EQ, value=f'{asset_naam}')], logicalOp=LogicalOpEnum.AND)
            ]))
    assets_list = list(eminfra_client.search_assets(query_dto=query_dto, actief=True))

    nbr_assets = len(assets_list)
    if nbr_assets > 1:
        logging.critical(f'Er bestaan meerdere assets (#{nbr_assets}) van het type: {typeURI}, met naam: {asset_naam}')
    elif nbr_assets == 1:
        logging.debug(f'Asset {asset_naam} ({typeURI}) bestaat al.')
        asset = assets_list[0]
    elif nbr_assets == 0:
        logging.debug(f'Asset {asset_naam} ({typeURI}) bestaat nog niet en wordt aangemaakt.')
        asset = next(eminfra_client.create_asset(
                parent_uuid=parent_uuid,
                naam=asset_naam,
                typeUuid=assettype_uuid,
                parent_asset_type=parent_asset_type
            ), None)
    else:
        logging.critical('Unknown error')

    return asset

def create_relatie_if_missing(bronAsset_uuid: str, doelAsset_uuid: str, relatie_naam: str) -> str:
    """
    Maak een relatie aan tussen 2 assets indien deze nog niet bestaat.
    Geeft de relatie-uuid weeer.

    :param bronAsset_uuid:
    :param doelAsset_uuid:
    :param relatie_naam:
    :return:
    """
    if response := eminfra_client.search_assetrelaties_OTL(
        bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid
    ):
        logging.debug(f'{relatie_naam}-relatie tussen {bronAsset_uuid} en {doelAsset_uuid} bestaat al. Returns relatie-uuid')
        relatie_uuid = response[0].get("RelatieObject.assetId").get("DtcIdentificator.identificator")[:36] # eerste 36 karakters van de UUID
    else:
        logging.debug(f'{relatie_naam}-relatie tussen {bronAsset_uuid} en {doelAsset_uuid} wordt aangemaakt. Returns relatie-uuid')
        relatieType_uuid = get_relatietype_uuid(relatie_naam=relatie_naam)
        relatie_uuid = eminfra_client.create_assetrelatie(bronAsset_uuid=bronAsset_uuid, doelAsset_uuid=doelAsset_uuid, relatieType_uuid=relatieType_uuid)
    return relatie_uuid

def get_current_typeURI(typeURI: str) -> str:
    """
    Retreives the typeURI (OTL or Legacy) that is actually in-place.

    Maps the key (typeURI OTL) to its corresponding value (typeURI Legacy).
    If the Legacy-asset is no longer in place, the value of the dictionary is empty and the key is returned.
    :param typeURI:
    :return:
    """
    dict = {
        "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Wegkantkast": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HSCabine": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy"
        , "HS": "https://lgc.data.wegenenverkeer.be/ns/installatie#HS"
        , "HSDeel": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel"
        , "LS": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS"
        , "LSDeel": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning": ""
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning": ""
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#EnergiemeterDNB": ""
        , "Switch": "https://lgc.data.wegenenverkeer.be/ns/installatie#Switch"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Segmentcontroller": "https://lgc.data.wegenenverkeer.be/ns/installatie#SegC"
        # , "": "https://lgc.data.wegenenverkeer.be/ns/installatie#WV"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule": "https://lgc.data.wegenenverkeer.be/ns/installatie#MIVLVE"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#MIVLus": "https://lgc.data.wegenenverkeer.be/ns/installatie#Mpt"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS": "https://lgc.data.wegenenverkeer.be/ns/installatie#RSSBord"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS": "https://lgc.data.wegenenverkeer.be/ns/installatie#RVMS"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS": "https://lgc.data.wegenenverkeer.be/ns/installatie#VMS"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera": "https://lgc.data.wegenenverkeer.be/ns/installatie#CameraLegacy"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug": "https://lgc.data.wegenenverkeer.be/ns/installatie#SeinbrugDVM"
        , "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal": ""
    }
    return [dict[typeURI] if dict.get(typeURI) != '' else typeURI for _ in dict.keys()]
def get_assettype_uuid(assettype_URI: str) -> str:
    """
    Returns the assettype_uuid that corresponds with an assettype_URI.
    The reverse mapping is based on a dictionary.
    :param assettype_URI:
    :return:
    """
    assettypes_dict = {
        '10377658-776f-4c21-a294-6c740b9f655e': "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "c3601915-3b66-4bde-9728-25c1bbf2f374": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Wegkantkast",
        "1cf24e76-5bf3-44b0-8332-a47ab126b87e": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSCabineLegacy",
        "d76cbedd-5488-428c-a221-fe0bc8f74fa2": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HSCabine",
        "46dcd9b1-f660-4c8c-8e3e-9cf794b4de75": "https://lgc.data.wegenenverkeer.be/ns/installatie#HS",
        "a9655f50-3de7-4c18-aa25-181c372486b1": "https://lgc.data.wegenenverkeer.be/ns/installatie#HSDeel",
        "80fdf1b4-e311-4270-92ba-6367d2a42d47": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
        "b4361a72-e1d5-41c5-bfcc-d48f459f4048": "https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel",
        "8e9307e2-4dd6-4a46-a298-dd0bc8b34236": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning",
        "b4ee4ea9-edd1-4093-bce1-d58918aee281": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBLaagspanning",
        "e77befed-4530-4d57-bdb9-426bdae4775d": "https://lgc.data.wegenenverkeer.be/ns/installatie#Switch",
        "f625b904-befc-4685-9dd8-15a20b23a58b": "https://lgc.data.wegenenverkeer.be/ns/installatie#SegC",
        "6c1883d1-7e50-441a-854c-b53552001e5f": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Segmentcontroller",
        "55362c2a-be7b-4efc-9437-765b351c8c51": "https://lgc.data.wegenenverkeer.be/ns/installatie#WV",
        "478add39-e6fb-4b0b-b090-9c65e836f3a0": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
        "4dfad588-277c-480f-8cdc-0889cfaf9c78": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
        "7f59b64e-9d6c-4ac9-8de7-a279973c9210": "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule",
        "a4c75282-972a-4132-ad72-0d0de09dbdb8": "https://lgc.data.wegenenverkeer.be/ns/installatie#MIVLVE",
        "63b42487-8f07-4d9a-823e-c5a5f3c0aa81": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#MIVLus",
        "dc3db3b7-7aad-4d7f-a788-a4978f803021": "https://lgc.data.wegenenverkeer.be/ns/installatie#Mpt",
        "9826b683-02fa-4d97-8680-fbabc91a417f": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRSS",
        "1496b2fd-0742-44a9-a3b4-e994bd5af8af": "https://lgc.data.wegenenverkeer.be/ns/installatie#RSSBord",
        "50f7400a-2e67-4550-b135-08cde6f6d64f": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordVMS",
        "ac837aa9-65bc-4c7c-b1c2-8ec0201a0203": "https://lgc.data.wegenenverkeer.be/ns/installatie#VMS",
        "0515e9bc-1778-43ae-81a9-44df3e2b7c21": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordRVMS",
        "5b44cb96-3edf-4ef5-bc85-ec4d5c5152a3": "https://lgc.data.wegenenverkeer.be/ns/installatie#RVMS",
        "3f98f53a-b435-4a69-af3c-cede1cd373a7": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera",
        "f66d1ad1-4247-4d99-80bb-5a2e6331eb96": "https://lgc.data.wegenenverkeer.be/ns/installatie#CameraLegacy",
        "40b2e487-f4b8-48a2-be9d-e68263bab75a": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Seinbrug",
        "6f66dad8-8290-4d07-8e8b-6add6c7fe723": "https://lgc.data.wegenenverkeer.be/ns/installatie#SeinbrugDVM",
        "615356ae-64eb-4a7d-8f40-6e496ec5b8d7": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Galgpaal"
    }
    assettype_uuids = [key for key, value in assettypes_dict.items() if value == assettype_URI]
    if len(assettype_uuids) != 1:
        logging.critical(f'assettype_URI: {assettype_URI} kon niet worden teruggevonden in de dictionary.')
    return assettype_uuids[0]

def get_relatietype_uuid(relatie_naam: str) -> str:
    """
    Returns the relatietype uuid.
    :param relatie_naam:
    :return:
    """
    relatietypes_dict = {
        "3ff9bf1c-d852-442e-a044-6200fe064b20": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
        "f2c5c4a1-0899-4053-b3b3-2d662c717b44": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt",
        "93c88f93-6e8c-4af3-a723-7e7a6d6956ac": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing",
        "812dd4f3-c34e-43d1-88f1-3bcd0b1e89c2": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij"
    }
    relatietype_uuids = [key for key, value in relatietypes_dict.items() if value == relatie_naam]
    if len(relatietype_uuids) != 1:
        logging.critical(f'relatietype_uuid: {relatie_naam} kon niet worden teruggevonden in de dictionary.')
    return relatietype_uuids[0]

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
        installatie_naam = match[1] + 'X' + temp_installatie_naam[match.end():]
    else:
        raise ValueError("De syntax van de kast bevat geen letter 'P', 'N' of 'M'.")

    return installatie_naam


def validate_asset(uuid: str = None, naam: str = None, stop_on_error: bool = True) -> None:
    """
    Controleer het bestaan van een asset op basis van diens uuid.
    Valideer nadien of de nieuwe naam overeenstemt met de naam van de bestaande asset.

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
            raise ValueError(f'Asset {uuid} werd niet teruggevonden in em-infra. Dit zou moeten bestaan.')

    if str(naam) != str(asset.naam):
        logging.error(
            f'Asset {uuid} naam {naam} komt niet overeen met de bestaande naam {asset.naam}.')
        if stop_on_error:
            raise ValueError(
                f'Asset {uuid} naam {naam} komt niet overeen met de bestaande naam {asset.naam}.')
    return None


def parse_wkt_geometry(asset_row):
    asset_row_x = asset_row.get('Positie X (Lambert 72)')
    asset_row_y = asset_row.get('Positie Y (Lambert 72)')
    asset_row_z = asset_row.get('Positie Z (Lambert 72, optioneel)')
    if asset_row_z is None:
        asset_row_z = 0
    if asset_row_x is None or asset_row_y is None:
        return None
    return f'POINT Z ({asset_row_x} {asset_row_y} {asset_row_z})'


def add_bestekkoppeling_if_missing(asset_uuid: str, eDelta_dossiernummer: str, start_datetime: datetime) -> None:
    """
    Voeg een specifieke bestekkoppeling toe, indien die nog niet bestaat bij een bepaalde asset.

    :param asset_uuid:
    :param eDelta_dossiernummer:
    :param start_datetime:
    :return:
    """
    # check if the eDelta_dossiernummer is valid.
    bestekref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(eDelta_dossiernummer=eDelta_dossiernummer)
    if bestekref is None:
        logging.critical(f'Bestek met eDelta_dossiernumer {eDelta_dossiernummer} werd niet teruggevonden. Omgeving: {environment.name}')

    huidige_bestekkoppelingen = eminfra_client.get_bestekkoppelingen_by_asset_uuid(asset_uuid=asset_uuid)
    # check if there are currently no bestekkkoppelingen.
    if all(
            bestekkoppeling.bestekRef.eDeltaDossiernummer
            != eDelta_dossiernummer
            for bestekkoppeling in huidige_bestekkoppelingen
    ):
        eminfra_client.add_bestekkoppeling(asset_uuid=asset_uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                           start_datetime=datetime(2024, 9, 1))


def zoek_verweven_asset(bron_uuid: str) -> AssetDTO | None:
    """
    Zoek de OTL-asset op basis van een Legacy-asset die verbonden zijn via een Gemigreerd-relatie.
    Returns None indien de Gemigreerd-relatie ontbreekt.

    :param bron_uuid: uuid van de bron asset (Legacy)
    :return:
    """
    relaties = eminfra_client.search_assetrelaties_OTL(bronAsset_uuid=bron_uuid)
    relatie_gemigreerd = [item for item in relaties if
                          item.get('@type') == 'https://lgc.data.wegenenverkeer.be/ns/onderdeel#GemigreerdNaar'][0]
    asset_uuid_gemigreerd = relatie_gemigreerd.get('RelatieObject.doelAssetId').get('DtcIdentificator.identificator')[
                            :36]
    return next(
        eminfra_client.search_asset_by_uuid(asset_uuid=asset_uuid_gemigreerd),
        None,
    )


def append_columns(df: pd.DataFrame, columns: list = ["asset_uuid"]) -> pd.DataFrame:
    """
    Append new columns to the dataframe with default value None.
    :param df: Dataframe
    :param columns: New columns
    :return: Dataframe
    """
    # append new columns
    for col in columns:
        df[col] = None
    return df


def import_data(filepath: Path):
    """
    Import data from Excel into a Pandas dataframe. Validate data. Append attributes (uuid's)

    Initiate global variables to store pandas dataframes

    :param filepath:
    :return: None
    """
    global df_assets_installaties
    df_assets_installaties = import_data_as_dataframe(filepath=filepath, sheet_name="Wegkantkasten")
    df_assets_installaties = append_columns(df=df_assets_installaties, columns=["installatie_uuid", "installatie_naam"])

    global df_assets_wegkantkasten
    df_assets_wegkantkasten = import_data_as_dataframe(filepath=filepath, sheet_name="Wegkantkasten")
    df_assets_wegkantkasten = append_columns(df=df_assets_wegkantkasten, columns=["asset_uuid"])

    global df_assets_voeding_HS_cabine
    df_assets_voeding = import_data_as_dataframe(filepath=filepath, sheet_name="HSCabines-CC-SC-HS-LS-Switch-WV")
    df_assets_voeding_HS_cabine = df_assets_voeding.loc[:, ['HSCabine_Object naam ROCO', 'HSCabine_Object assetId.toegekendDoor', 'HSCabine_Object assetId.identificator', 'HSCabine_UUID Object', 'HSCabine_Object typeURI', 'HSCabine_Positie X (Lambert 72)', 'HSCabine_Positie Y (Lambert 72)', 'HSCabine_Positie Z (Lambert 72)']]
    df_assets_voeding_HS_cabine = append_columns(df=df_assets_voeding_HS_cabine, columns=["asset_uuid"])
    global df_assets_voeding_hoogspanningsdeel
    df_assets_voeding_hoogspanningsdeel = df_assets_voeding.loc[:, ['Hoogspanningsdeel_HSDeel aanwezig (Ja/Nee)', 'Hoogspanningsdeel_Naam HSDeel', 'Hoogspanningsdeel_HSDeel lgc:installatie', 'Hoogspanningsdeel_UUID HSDeel', 'Bevestigingsrelatie HSDeel_Bevestigingsrelatie assetId.identificator', 'Bevestigingsrelatie HSDeel_Bevestigingsrelatie typeURI', 'Bevestigingsrelatie HSDeel_UUID Bevestigingsrelatie']]
    df_assets_voeding_hoogspanningsdeel = append_columns(df=df_assets_voeding_hoogspanningsdeel,
                                                         columns=["asset_uuid", "bevestigingsrelatie_uuid",
                                                                  "voedingsrelatie_uuid"])
    global df_assets_voeding_laagspanningsdeel
    df_assets_voeding_laagspanningsdeel = df_assets_voeding.loc[:, ['Laagspanningsdeel_LSDeel aanwezig (Ja/Nee)', 'Laagspanningsdeel_Naam LSDeel', 'Laagspanningsdeel_LSDeel lgc:installatie', 'Laagspanningsdeel_UUID LSDeel', 'Bevestigingsrelatie LSDeel_Bevestigingsrelatie assetId.identificator', 'Bevestigingsrelatie LSDeel_Bevestigingsrelatie typeURI', 'Bevestigingsrelatie LSDeel_UUID Bevestigingsrelatie', 'Voedingsrelatie HSDeel naar LSDeel_Voedingsrelatie assetId.identificator', 'Voedingsrelatie HSDeel naar LSDeel_Voedingsrelatie typeURI', 'Voedingsrelatie HSDeel naar LSDeel_UUID Voedingsrelatie bronAsset', 'Voedingsrelatie HSDeel naar LSDeel_UUID Voedingsrelatie']]
    df_assets_voeding_laagspanningsdeel = append_columns(df=df_assets_voeding_laagspanningsdeel,
                                                         columns=["asset_uuid", "bevestigingsrelatie_uuid"])
    global df_assets_voeding_hoogspanning
    df_assets_voeding_hoogspanning = df_assets_voeding.loc[:, ['Hoogspanning_HS aanwezig (Ja/Nee)', 'Hoogspanning_Naam HS', 'Hoogspanning_HS lgc:installatie', 'Hoogspanning_UUID HS', 'Bevestigingsrelatie HS_Bevestigingsrelatie assetId.identificator', 'Bevestigingsrelatie HS_Bevestigingsrelatie typeURI', 'Bevestigingsrelatie HS_UUID Bevestigingsrelatie']]
    df_assets_voeding_hoogspanning = append_columns(df=df_assets_voeding_hoogspanning,
                                                    columns=["asset_uuid", "bevestigingsrelatie_uuid"])
    global df_assets_voeding_DNB_hoogspanning
    df_assets_voeding_DNB_hoogspanning = df_assets_voeding.loc[:, ['DNBHoogspanning_Object assetId.identificator', 'DNBHoogspanning_UUID Object', 'DNBHoogspanning_Object typeURI', 'DNBHoogspanning_eanNummer', 'DNBHoogspanning_referentieDNB', 'HoortBij Relatie voor DNBHoogspanning_HoortBij assetId.identificator', 'HoortBij Relatie voor DNBHoogspanning_HoortBij typeURI', 'HoortBij Relatie voor DNBHoogspanning_UUID HoortBijrelatie']]
    df_assets_voeding_DNB_hoogspanning = append_columns(df=df_assets_voeding_DNB_hoogspanning,
                                                        columns=["asset_uuid", "hoortbijrelatie_uuid"])
    global df_assets_voeding_energiemeter_DNB
    df_assets_voeding_energiemeter_DNB = df_assets_voeding.loc[:, ['EnergiemeterDNB_Object assetId.identificator', 'EnergiemeterDNB_UUID Object', 'EnergiemeterDNB_Object typeURI', 'EnergiemeterDNB_meternummer', 'HoortBij Relatie voor EnergiemeterDNB_HoortBij assetId.identificator', 'HoortBij Relatie voor EnergiemeterDNB_HoortBij typeURI', 'HoortBij Relatie voor EnergiemeterDNB_UUID HoortBijrelatie']]
    df_assets_voeding_energiemeter_DNB = append_columns(df=df_assets_voeding_energiemeter_DNB,
                                                        columns=["asset_uuid", "hoortbijrelatie_uuid"])
    global df_assets_voeding_segmentcontroller
    df_assets_voeding_segmentcontroller = df_assets_voeding.loc[:, ['Segmentcontroller_Naam SC', 'Segmentcontroller_SC TypeURI', 'Segmentcontroller_UUID SC']]
    df_assets_voeding_segmentcontroller = append_columns(df=df_assets_voeding_segmentcontroller, columns=["asset_uuid"])
    global df_assets_voeding_wegverlichting
    df_assets_voeding_wegverlichting = df_assets_voeding.loc[:, ['Wegverlichtingsgroep_WV aanwezig (Ja/Nee)', 'Wegverlichtingsgroep_Naam WV', 'Wegverlichtingsgroep_WV  lgc:installatie', 'Wegverlichtingsgroep_UUID WV']]
    df_assets_voeding_wegverlichting = append_columns(df=df_assets_voeding_wegverlichting, columns=["asset_uuid"])
    global df_assets_voeding_switch
    df_assets_voeding_switch = df_assets_voeding.loc[:, ['Switch gegevens_Switch aanwezig (Ja/Nee)', 'Switch gegevens_Object assetId.toegekendDoor', 'Switch gegevens_Object assetId.identificator', 'Switch gegevens_UUID switch', 'Switch gegevens_Aantal poorten', 'Switch gegevens_Glasvezellus']]
    df_assets_voeding_switch = append_columns(df=df_assets_voeding_switch, columns=["asset_uuid"])

    global df_assets_openbare_verlichting
    df_assets_openbare_verlichting = import_data_as_dataframe(filepath=filepath, sheet_name="Openbare verlichting")
    df_assets_openbare_verlichting = append_columns(df=df_assets_openbare_verlichting,
                                                    columns=["asset_uuid", "voedingsrelatie_uuid"])

    global df_assets_mivlve
    df_assets_mivlve = import_data_as_dataframe(filepath=filepath, sheet_name="MIVLVE")
    df_assets_mivlve = append_columns(df=df_assets_mivlve, columns=["asset_uuid", "bevestigingsrelatie_uuid"])

    global df_assets_mivmeetpunten
    df_assets_mivmeetpunten = import_data_as_dataframe(filepath=filepath, sheet_name="MIVMeetpunten")

    global df_assets_RSS_borden
    df_assets_mivmeetpunten = append_columns(df=df_assets_mivmeetpunten, columns=["asset_uuid", "sturingsrelatie_uuid"])
    df_assets_RRS_borden = import_data_as_dataframe(filepath=filepath, sheet_name="RSS-borden")
    df_assets_RSS_borden = append_columns(df=df_assets_RRS_borden,
                                          columns=["asset_uuid", "hoortbijrelatie_uuid", "bevestigingsrelatie_uuid",
                                                   "voedingsrelatie_uuid"])

    global df_assets_RVMS_borden
    df_assets_RVMS_borden = import_data_as_dataframe(filepath=filepath, sheet_name="(R)VMS-borden")
    df_assets_RVMS_borden = append_columns(df=df_assets_RVMS_borden,
                                           columns=["asset_uuid", "hoortbijrelatie_uuid", "bevestigingsrelatie_uuid",
                                                    "voedingsrelatie_uuid"])

    global df_assets_cameras
    df_assets_cameras = import_data_as_dataframe(filepath=filepath, sheet_name="Cameras")
    df_assets_cameras = append_columns(df=df_assets_cameras,
                                       columns=["asset_uuid", "voedingsrelatie_uuid", "bevestigingsrelatie_uuid"])

    global df_assets_portieken_seinbruggen
    df_assets_portieken_seinbruggen = import_data_as_dataframe(filepath=filepath, sheet_name="Portieken-Seinbruggen")
    df_assets_portieken_seinbruggen = append_columns(df=df_assets_portieken_seinbruggen, columns=["asset_uuid"])

    global df_assets_galgpaal
    df_assets_galgpaal = import_data_as_dataframe(filepath=filepath, sheet_name="Galgpaal")
    df_assets_galgpaal = append_columns(df=df_assets_galgpaal, columns=["asset_uuid"])


def process_installatie(df: pd.DataFrame):
    for idx, asset_row in df.iterrows():
        asset_row_naam = asset_row.get("Wegkantkast_Object assetId.identificator")
        installatie_naam = construct_installatie_naam(kastnaam=asset_row_naam)
        df.at[idx, "installatie_naam"] = installatie_naam
        df.at[idx, "installatie_uuid"] = create_installatie_if_missing(naam=installatie_naam)


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Lantis Bypass: \tAanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding')

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    eDelta_dossiernummer = 'INTERN-095'
    logging.info(f'Bestekkoppeling: {eDelta_dossiernummer}')

    excel_file = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250507.xlsx'
    logging.info(f"Excel file wordt ingelezen en gevalideerd: {excel_file}")

    output_excel_path = Path(__file__).resolve().parent / 'data' / 'output' / f'lantis_bypass_{datetime.now().strftime(format="%Y-%m-%d")}.xlsx'
    logging.info(f'Output file path: {output_excel_path}')

    logging.info('Import data, validate, and prepare the dataframes.')
    import_data(filepath=excel_file)


    logging.info('Aanmaken van installaties op basis van de kastnamen')
    process_installatie(df=df_assets_installaties)
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        df_assets_installaties.to_excel(writer, sheet_name='Installaties',
                                        columns=["installatie_uuid", "installatie_naam"],
                                        index=False, freeze_panes=[1, 1])
    logging.info('Installaties aangemaakt')

    # todo wrap alle code tot het wegschrijven naar Excel in een functie: process_wegkantkasten.
    # process_wegkantkasten(df=df_assets_wegkankasten)
    logging.info('Aanmaken van Wegkantkasten onder installaties')
    for idx, asset_row in df_assets_wegkantkasten.iterrows():
        asset_row_uuid = asset_row.get("Wegkantkast_UUID Object")
        asset_row_typeURI = asset_row.get("Wegkantkast_Object typeURI")
        asset_row_name = asset_row.get("Wegkantkast_Object assetId.identificator")
        asset_row_parent_name = construct_installatie_naam(kastnaam=asset_row_name)
        parent_asset = next(eminfra_client.search_beheerobjecten(naam=asset_row_parent_name, actief=True, operator=OperatorEnum.EQ), None)

        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

        if asset_row_uuid and asset_row_name:
            logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
            validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

        if parent_asset is None:
            logging.critical(f'Parent asset is ongekend.')

        asset = create_asset_if_missing(typeURI=asset_row_typeURI, parent_uuid=parent_asset.uuid, asset_naam=asset_row_name, parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

        # Update toestand
        if asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW}"')
            eminfra_client.update_toestand(asset=asset, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        if asset_row_wkt_geometry := parse_wkt_geometry(asset_row=asset_row):
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # Add bestekkoppelingen
        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        df_assets_wegkantkasten.at[idx, "asset_uuid"] = asset.uuid
    # Wegschrijven van het dataframe
    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_assets_wegkantkasten.to_excel(writer, sheet_name='Wegkantkasten', columns=["Wegkantkast_Object assetId.identificator", "asset_uuid"], index=False, freeze_panes=[1, 1])
    logging.info('Wegkantkasten aangemaakt')


    # Aanmaken van de MIVLVE
    logging.info('Aanmaken van MIVLVE onder Wegkantkasten')
    for idx, asset_row in df_assets_mivlve.iterrows():
        asset_row_uuid = asset_row.get("LVE_UUID Object")
        asset_row_typeURI = asset_row.get("LVE_Object typeURI")
        asset_row_name = asset_row.get("LVE_Object assetId.identificator")
        asset_row_parent_name = asset_row.get("Bevestigingsrelatie doelAssetId.identificator")
        parent_asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)

        logging.debug(f'Processing asset {idx}. uuid: {asset_row_uuid}, name: {asset_row_name}')

        if asset_row_uuid and asset_row_name:
            logging.info('Valideer asset waarvoor reeds een uuid én een naam gekend is.')
            validate_asset(uuid=asset_row_uuid, naam=asset_row_name, stop_on_error=True)

        if parent_asset is None:
            logging.critical(f'Parent asset is ongekend.')

        asset = create_asset_if_missing(typeURI=asset_row_typeURI, parent_uuid=parent_asset.uuid,
                                        asset_naam=asset_row_name,
                                        parent_asset_type=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

        # Update toestand
        if asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW}"')
            eminfra_client.update_toestand(asset=asset, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        if asset_row_wkt_geometry := parse_wkt_geometry(asset_row=asset_row):
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # todo: wrap onderstaand stuk code in een functie
        # Bevestiging-relatie
        doelAsset_uuid = asset_row.get('UUID Bevestigingsrelatie doelAsset')
        create_relatie_if_missing(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid, relatie_naam='Bevestiging')
        # todo verwijder onderstaande code nadat de functie werd getest.
        # if doelAsset_uuid:
        #     # check of de relatie al bestaat.
        #     response = eminfra_client.search_assetrelaties_OTL(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid)
        #     if not response:
        #         relatieType_uuid = get_relatietype_uuid(relatie_naam='Bevestiging')
        #         relatie_uuid = eminfra_client.create_assetrelatie(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid, relatieType_uuid=relatieType_uuid)
        #     else:
        #         relatie_uuid = response[0].get("RelatieObject.assetId").get("DtcIdentificator.identificator")[:36] # eerste 36 karakters van de UUID
        # else:
        #     relatie_uuid = None

        # todo tot hier (eigenschappen)
        # update eigenschap XXX

        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        verweven_asset = zoek_verweven_asset(bron_uuid=asset.uuid)

        add_bestekkoppeling_if_missing(asset_uuid=verweven_asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        df_assets_mivlve.at[idx, "asset_uuid"] = asset.uuid
        df_assets_mivlve.at[idx, "bevestigingsrelatie_uuid"] = relatie_uuid

    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_assets_mivlve.to_excel(writer, sheet_name='MIVLVE', columns=["Object assetId.identificator", "asset_uuid", "bevestigingsrelatie_uuid"], index=False, freeze_panes=[1, 1])
    logging.info('MIVLVE aangemaakt')

    # Aanmaken van de MIVMeetpunten
    logging.info('Aanmaken van MIVMeetpunten onder MIVLVE')
    for idx, asset_row in df_assets_mivmeetpunten.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("Object assetId.identificator")
        assettype_uuid = get_assettype_uuid(assettype_URI='MIVMeetpunten')
        assettype_uuid_parent = get_assettype_uuid(assettype_URI='MIVLVE')
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
            # raise ValueError(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVMeetpunt "{asset_row_uuid}" en diens Sturing-relatie')
        else:
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
                ExpressionDTO(terms=[TermDTO(property='naam', operator=OperatorEnum.CONTAINS, value=f'{asset_row_naam}')], logicalOp=LogicalOpEnum.AND)
            ]))
        # Het volledige naampad is niet gekend, dus zoek in de resultaten lijst of de uuid van de parent-asset overeenkomt.
        asset_candidates = eminfra_client.search_assets(query_dto=query)
        asset = next((asset for asset in asset_candidates if asset.parent.uuid == asset_row_parent_asset_uuid), None)
        if asset is None:
            logging.debug(f'Asset met als naam "{asset_row_naam}" bestaat niet en wordt aangemaakt')
            asset_dict = eminfra_client.create_asset(
                parent_uuid=asset_row_parent_asset_uuid
                , naam=asset_row_naam
                , typeUuid=assettype_uuid
                , parent_asset_type=BoomstructuurAssetTypeEnum.ASSET
            )
            try:
                asset_row_uuid = asset_dict.get("uuid")
                asset = next(eminfra_client.search_asset_by_uuid(asset_uuid=asset_row_uuid), None)
            except:
                logging.critical(f'Asset werd niet teruggevonden in em-infra: {asset_row_uuid}')
                raise ValueError(f'Asset werd niet teruggevonden in em-infra: {asset_row_uuid}')

        if asset is None:
            logging.critical('Asset werd niet aangemaakt')

        # Update toestand
        if asset.toestand.value != AssetDTOToestand.IN_OPBOUW.value:
            logging.debug(f'Update toestand: "{asset.uuid}": "{AssetDTOToestand.IN_OPBOUW}"')
            eminfra_client.update_toestand(asset=asset, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        if asset_row_wkt_geometry := parse_wkt_geometry(asset_row=asset_row):
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # Sturing-relatie
        # ! De Sturing-relatie loopt van een meetpunt (bron) naar een meetlus MIVLVE (doel)
        bronAsset_uuid = asset_row.get('UUID Sturingsrelatie bronAsset')
        if bronAsset_uuid:
            # check of de relatie al bestaat.
            response = eminfra_client.search_assetrelaties_OTL(bronAsset_uuid=asset.uuid, doelAsset_uuid=bronAsset_uuid)
            if not response:
                relatieType_uuid = get_relatietype_uuid(relatie_naam='Sturing')
                relatie_uuid = eminfra_client.create_assetrelatie(bronAsset_uuid=asset.uuid, doelAsset_uuid=bronAsset_uuid, relatieType_uuid=relatieType_uuid)
            else:
                relatie_uuid = response[0].get("RelatieObject.assetId").get("DtcIdentificator.identificator")[:36] # eerste 36 karakters van de UUID
        else:
            relatie_uuid = None

        # update eigenschappen: Aansluiting, Formaat, Laag, Uitslijprichting, Wegdek
        # eigenschapwaarden_huidig = eminfra_client.get_eigenschapwaarden(assetId=asset.uuid)
        # eigenschappen_huidig = eminfra_client.get_eigenschappen(assetId=asset.uuid)
        #
        # eigenschapnamen_meetpunt = ['Aansluiting', 'Formaat', 'Laag', 'Uitslijprichting', 'Wegdek']
        # for eigenschapnaam_meetpunt in eigenschapnamen_meetpunt:
        #     logging.debug(f'Update eigenschap {eigenschapnaam_meetpunt}')
        #     asset_row_eigenschap_nieuw = asset_row[f'{eigenschapnaam_meetpunt}']
        #     # zoek de huidige waarde voor deze eigenschap, indien onbestaande, zoek de Eigenschap en maak er een een Eigenschapwaarde van.
        #
        #     eigenschapwaarde_huidig = next((eigenschapwaarde for eigenschapwaarde in eigenschapwaarden_huidig if eigenschapwaarde.eigenschap.naam == eigenschapnaam_meetpunt), None)
        #     # todo implementeer de situatie waarbij de eigenschap nog niet bestaat. Ophalen van de eigenschap en nadien de waarde toekennen in TypedValue.
        #     if eigenschapwaarde_huidig is None:
        #         eigenschap_huidig = eminfra_client.search_eigenschappen(eigenschap_naam=eigenschapnaam_meetpunt)
        #         eigenschap_huidig = next((eigenschap for eigenschap in eminfra_client.search_eigenschappen(eigenschap_naam=eigenschapnaam_meetpunt) if 'https://lgc.data.wegenenverkeer.be' in eigenschap.uri), None) # beperk de eigenschappen tot de Legacy eigenschappen.
        #         # todo implement check indien er meerdere gelijknamige eigenschappen bestaan
        #
        #     if eigenschapwaarde_huidig and eigenschapwaarde_huidig.typedValue.get('value') != asset_row_eigenschap_nieuw:
        #         eigenschapwaarde_nieuw = copy.deepcopy(eigenschapwaarde_huidig)
        #         eigenschapwaarde_nieuw.typedValue['value'] = asset_row_eigenschap_nieuw
        #
        #         eminfra_client.update_eigenschap(assetId=asset.uuid, eigenschap=eigenschapwaarde_nieuw)

        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        verweven_asset = zoek_verweven_asset(bron_uuid=asset.uuid)

        add_bestekkoppeling_if_missing(asset_uuid=verweven_asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        df_assets_mivmeetpunten.at[idx, "asset_uuid"] = asset.uuid
        df_assets_mivmeetpunten.at[idx, "sturingsrelatie_uuid"] = relatie_uuid
    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_assets_mivmeetpunten.to_excel(writer, sheet_name='MIVMeetpunten',
                                         columns=["Object assetId.identificator", "asset_uuid",
                                                  "sturingsrelatie_uuid"], index=False, freeze_panes=[1, 1])
    logging.info('MIVMeetpunten aangemaakt')