import shutil
import tempfile
from datetime import datetime

import pandas as pd
import re

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import DocumentCategorieEnum, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, \
    OperatorEnum, ExpansionsDTO, construct_naampad, LogicalOpEnum, ApplicationEnum, ProvincieEnum
from pathlib import Path
from API.Enums import AuthType, Environment
from Generic.ExcelModifier import ExcelModifier

def download_documents(eminfra_client, edelta_dossiernummer: str, document_categorie:[DocumentCategorieEnum], toezichter:str = None, provincie:[ProvincieEnum] = None):
    """Download documents

    Downloading documents from EM-Infra based on the criteria:
        edelta_dossiernummer, document_categorien, toezichter (optional), provincie (optional)

    :param eminfra_client: EMInfraClient Client voor de authenticatie met EM-Infra
    :param edelta_dossiernummer: str Dossiernummer
    :param document_categorie: [DocumentCategorieEnum] Lijst van Document categoriën
    :param toezichter: str Naam van de toezichter
    :param provincie: [ProvincieEnum] Lijst van provincienamen
    """
    provincie_value = [item.value for item in provincie] if provincie else provincie
    document_categorie_value = [item.value for item in document_categorien]
    print(f'Ophalen van alle documenten die voldoen aan volgende criteria:'
          f'\tDocument categorie: {document_categorie_value}'
          f'\tDossiernummer: {edelta_dossiernummer}'
          f'\tToezichter: {toezichter}'
          f'\tProvincie: {provincie_value}')

    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(edelta_dossiernummer)
    bestek_ref_uuid = bestek_ref[0].uuid

    # build query to search assets linked with an order (NL: bestek)
    query_dto_search_assets = QueryDTO(
        size=5,
        from_=0,
        pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[
                        TermDTO(
                            property='actiefBestek',
                            operator=OperatorEnum.EQ,
                            value=f'{bestek_ref_uuid}',
                            logicalOp=None)]
                )]
        )
        , expansions=ExpansionsDTO(
            fields=["parent", "kenmerk:f0166ba2-757c-4cf3-bf71-2e4fdff43fa3"])
    )

    # Append "toezichter" to the search-query
    if toezichter:
        # Voeg álle toezichters toe in de zoekopdracht
        identiteiten = eminfra_client.search_identiteit(naam=toezichter)
        identiteiten_uuid = [identiteit.uuid for identiteit in identiteiten]
        query_dto_search_assets.selection.expressions[0].terms.append(
            TermDTO(property='toezichter',operator=OperatorEnum.IN,value=identiteiten_uuid,logicalOp=LogicalOpEnum.AND)
        )

    # Append "provincie" to the search-query
    if provincie_value:
        query_dto_search_assets.selection.expressions[0].terms.append(
            TermDTO(
                property='locatieProvincie',operator=OperatorEnum.IN,value=provincie_value,logicalOp=LogicalOpEnum.AND)
        )

    # build query to download documents
    query_dto_search_document = QueryDTO(
        size=100
        , from_=0
        , pagingMode=PagingModeEnum.OFFSET
        , selection=SelectionDTO(
            expressions=[ExpressionDTO(
                terms=[TermDTO(property='categorie',operator=OperatorEnum.IN,value=document_categorie_value)]
            )]
        )
    )

    # Create temporary folder, download all files, write an overview in an Excel file and zip all results to an output folder (Downloads).
    downloads_path = Path.home() / 'Downloads' / 'Results'
    print("Downloading documents...")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        asset_bucket_generator = eminfra_client.search_all_assets(query_dto_search_assets)

        # Store assets in a pandas dataframe
        df_assets = pd.DataFrame(
            columns=["uuid", "assettype", "naam", "naampad", "actief", "toestand",
                     "toezichter_naam", "toezichter_voornaam", "provincie", "gemeente",
                     "document_categorie", "document_naam", "document_uuid"])

        start_time = datetime.now()
        for i, asset in enumerate(asset_bucket_generator):
            # Track progress
            if i % 10 == 0:
                elapsed = datetime.now() - start_time
                print(f"Processed {i} assets in {elapsed.total_seconds():.2f} seconds")

            # locatiekenmerk
            locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset.uuid)
            if locatiekenmerk.locatie.get('adres'):  # Skip when locatiekenmerk.locatie is None
                locatie_adres = locatiekenmerk.locatie.get('adres')
                locatie_gemeente = locatie_adres.get('gemeente')
                if locatie_gemeente is None:
                    locatie_gemeente = 'ongekend'
                locatie_provincie = locatie_adres.get('provincie')
                if locatie_provincie is None:
                    locatie_provincie = 'ongekend'


            # identiteitkenmerk (toezichter)
            toezichter_uuid = asset.kenmerken.get('data')[0].get('toezichter').get('uuid')
            identiteit_kenmerk = eminfra_client.get_identiteit(toezichter_uuid)
            toezichter_naam = identiteit_kenmerk.naam
            toezichter_voornaam = identiteit_kenmerk.voornaam
            toezichter_volledige_naam = f'{toezichter_naam}_{toezichter_voornaam}'

            naampad = construct_naampad(asset)

            documents = eminfra_client.search_documents_by_asset_uuid(
                asset_uuid=asset.uuid
                , query_dto=query_dto_search_document
            )

            for document in documents:
                row_dict = {
                    'uuid': asset.uuid
                    , 'assettype': asset.type.afkorting
                    , 'naam': asset.naam
                    , 'naampad': naampad
                    , 'actief': asset.actief
                    , 'toestand': asset.toestand.value
                    , 'toezichter_naam': toezichter_naam
                    , 'toezichter_voornaam': toezichter_voornaam
                    , 'provincie': locatie_provincie
                    , 'gemeente': locatie_gemeente
                    , 'document_categorie': document.categorie.value
                    , 'document_naam': document.naam
                    , 'document_uuid': document.uuid
                    }

                row_df = pd.DataFrame([row_dict])
                df_assets = pd.concat([df_assets, row_df], ignore_index=True)

                # Write document to temp_dir
                eminfra_client.download_document(
                    document=document
                    , directory=temp_path / locatie_provincie / toezichter_volledige_naam / naampad.replace('/', '__') / document.categorie.value
                )

        # Write overview
        edelta_dossiernummer_str = re.sub('[^0-9a-zA-Z]+', '_', edelta_dossiernummer)  # replace all non-alphanumeric characters with an underscore
        output_file_path_excel = temp_path / 'overzicht.xlsx'
        df_assets.to_excel(excel_writer=output_file_path_excel
                           , sheet_name=edelta_dossiernummer_str
                           , index=False
                           , engine="openpyxl")

        excelModifier = ExcelModifier(output_file_path_excel)
        excelModifier.add_hyperlink(sheet=edelta_dossiernummer_str, link_type=ApplicationEnum.EM_INFRA, env=Environment.PRD)

        # Zip the output folder. Temp folder is automatically removed
        zip_path = downloads_path / f'documenten_{edelta_dossiernummer_str}'  # The output path of the zip file (without extension)
        shutil.make_archive(str(zip_path), 'zip', root_dir=str(temp_dir))
        print(f"Folder {temp_dir} has been zipped to {zip_path}.zip.")


if __name__ == '__main__':
    # settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    # eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    awv_acm_cookie = 'f08ac55a59af4fcf8bd923bf388598ed'  # 15/01/2025
    eminfra_client = EMInfraClient(cookie=awv_acm_cookie, auth_type=AuthType.COOKIE, env=Environment.PRD)

    # edelta_dossiernummer
    edelta_dossiernummer = 'VWT/DVM/2023/3'

    # document_categorien
    document_categorien = [DocumentCategorieEnum.KEURINGSVERSLAG, DocumentCategorieEnum.ELEKTRISCH_SCHEMA]
    print(f'De mogelijke document categoriën zijn: {[item.value for item in DocumentCategorieEnum]}')

    # toezichter
    toezichter = 'Stefan Missotten'

    # provincie
    print(f'De mogelijke provincies zijn: {[item.value for item in ProvincieEnum]}')
    # provincie = [ProvincieEnum.OOST_VLAANDEREN, ProvincieEnum.VLAAMS_BRABANT]
    provincie = None

    download_documents(eminfra_client=eminfra_client, edelta_dossiernummer=edelta_dossiernummer,
                       document_categorie=document_categorien, provincie=provincie, toezichter=toezichter)