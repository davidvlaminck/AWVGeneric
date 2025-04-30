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
    assettype_uuid = mapping_assettypes[mapping_key]
    if assettype_uuid == "":
        logging.warning(f"Mapping for asset types for key '{mapping_key}' is empty.")
    return assettype_uuid

def get_relatietype_uuid(mapping_key: str) -> str:
    """
    Returns the relatietype uuid.
    :param mapping_key:
    :return:
    """
    mapping_relatietypes = {
        "Bevestiging": "3ff9bf1c-d852-442e-a044-6200fe064b20",
        "Voedt": "f2c5c4a1-0899-4053-b3b3-2d662c717b44",
        "Sturing": "93c88f93-6e8c-4af3-a723-7e7a6d6956ac"
    }
    return mapping_relatietypes[mapping_key]


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


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s\t', filemode="w")
    logging.info('Lantis Bypass: \tAanmaken van assets en relaties voor de Bypass van de Oosterweelverbinding')

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    logging.info(f'settings_path: {settings_path}')

    environment = Environment.PRD
    logging.info(f'Omgeving: {environment.name}')

    eminfra_client = EMInfraClient(env=environment, auth_type=AuthType.JWT, settings_path=settings_path)

    eDelta_dossiernummer = 'INTERN-059'
    logging.info(f'Bestekkoppeling: {eDelta_dossiernummer}')

    excel_file = Path(__file__).resolve().parent / 'data' / 'input' / 'Componentenlijst_20250417_PRD.xlsx'
    logging.info(f"Excel file wordt ingelezen en gevalideerd: {excel_file}")

    output_excel_path = Path(__file__).resolve().parent / 'data' / 'output' / 'lantis_bypass.xlsx'
    logging.info(f'Output file path: {output_excel_path}')

    # Lees voor de installaties eveneens het tabblad "Wegkantkasten" om de installaties te instantiëren
    # Iedere wegkantkast maakt deel uit van een installatie. De installatienaam is gebaseerd op de naam van de wegkantkast
    df_assets_installaties = import_data_as_dataframe(filepath=excel_file, sheet_name="Wegkantkasten")

    df_assets_wegkantkasten = import_data_as_dataframe(filepath=excel_file, sheet_name="Wegkantkasten")

    df_assets_mivlve = import_data_as_dataframe(filepath=excel_file, sheet_name="MIVLVE")

    df_assets_mivmeetpunten = import_data_as_dataframe(filepath=excel_file, sheet_name="MIVMeetpunten")

    # Append new columns to the dataframes to fill with information (uuid) inside the iteration
    new_columns = ["installatie_uuid", "installatie_naam"]
    for col in new_columns:
        df_assets_installaties[col] = None
    new_columns = ["asset_uuid"]
    for col in new_columns:
        df_assets_wegkantkasten[col] = None
    new_columns = ["asset_uuid", "bevestigingsrelatie_uuid"]
    for col in new_columns:
        df_assets_mivlve[col] = None
    new_columns = ["asset_uuid", "sturingsrelatie_uuid"]
    for col in new_columns:
        df_assets_mivmeetpunten[col] = None

    # Aanmaken van de Installaties (beheerobject)
    logging.info('Aanmaken van installaties op basis van de kastnamen')
    for idx, asset_row in df_assets_installaties.iterrows():
        asset_row_naam = asset_row.get("Object assetId.identificator")
        installatie_naam = construct_installatie_naam(kastnaam=asset_row_naam)
        df_assets_installaties.at[idx, "installatie_naam"] = installatie_naam
        df_assets_installaties.at[idx, "installatie_uuid"] = create_installatie(naam=installatie_naam)
    # Check if the file exists
    if not Path(output_excel_path).exists():
        # Create new file
        with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
            df_assets_installaties.to_excel(writer, sheet_name='Installaties',
                                            columns=["installatie_uuid", "installatie_naam"], index=False,
                                            freeze_panes=[1, 1])
    logging.info('Installaties aangemaakt')


    # Aanmaken van de Wegkantkasten
    logging.info('Aanmaken van Wegkantkasten onder installaties')
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
            eminfra_client.update_toestand(asset=asset, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        if asset_row_wkt_geometry := parse_wkt_geometry(asset_row=asset_row):
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        add_bestekkoppeling_if_missing(asset_uuid=asset.uuid, eDelta_dossiernummer=eDelta_dossiernummer,
                                       start_datetime=datetime(2024, 9, 1))

        # Lijst aanvullen met de naam en diens overeenkomstig uuid
        df_assets_wegkantkasten.at[idx, "asset_uuid"] = asset.uuid
    # Wegschrijven van het dataframe
    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_assets_wegkantkasten.to_excel(writer, sheet_name='Wegkantkasten', columns=["Object assetId.identificator", "asset_uuid"], index=False, freeze_panes=[1, 1])
    logging.info('Wegkantkasten aangemaakt')


    # Aanmaken van de MIVLVE
    logging.info('Aanmaken van MIVLVE onder Wegkantkasten')
    for idx, asset_row in df_assets_mivlve.iterrows():
        asset_row_uuid = asset_row.get("UUID Object")
        asset_row_typeURI = asset_row.get("Object typeURI")
        asset_row_naam = asset_row.get("Object assetId.identificator")
        assettype_uuid = get_assettype_uuid(mapping_key='MIVLVE')
        asset_row_parent_name = asset_row.get("Bevestigingsrelatie doelAssetId.identificator")
        asset_row_parent_asset = next(eminfra_client.search_asset_by_name(asset_name=asset_row_parent_name, exact_search=True), None)
        if asset_row_parent_asset is None:
            logging.critical(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVLVE "{asset_row_uuid}" en diens bevestiging-relatie')
            # raise ValueError(f'Parent asset via de Bevestiging-relatie is onbestaande. Controleer MIVLVE "{asset_row_uuid}" en diens bevestiging-relatie')
        else:
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
            eminfra_client.update_toestand(asset=asset, toestand=AssetDTOToestand.IN_OPBOUW)

        # Update eigenschap locatie
        if asset_row_wkt_geometry := parse_wkt_geometry(asset_row=asset_row):
            logging.debug(f'Update eigenschap locatie: "{asset.uuid}": "{asset_row_wkt_geometry}"')
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.uuid, wkt_geom=asset_row_wkt_geometry)

        # Bevestiging-relatie
        doelAsset_uuid = asset_row.get('UUID Bevestigingsrelatie doelAsset')
        if doelAsset_uuid:
            # check of de relatie al bestaat.
            response = eminfra_client.search_assetrelaties_OTL(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid)
            if not response:
                relatieType_uuid = get_relatietype_uuid(mapping_key='Bevestiging')
                relatie_uuid = eminfra_client.create_assetrelatie(bronAsset_uuid=asset.uuid, doelAsset_uuid=doelAsset_uuid, relatieType_uuid=relatieType_uuid)
            else:
                relatie_uuid = response[0].get("RelatieObject.assetId").get("DtcIdentificator.identificator")[:36] # eerste 36 karakters van de UUID
        else:
            relatie_uuid = None

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
                relatieType_uuid = get_relatietype_uuid(mapping_key='Sturing')
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