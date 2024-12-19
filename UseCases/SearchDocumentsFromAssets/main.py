import itertools
import shutil
import tempfile
from itertools import islice

import pandas as pd
import re

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import DocumentCategorieEnum, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, \
    OperatorEnum, ExpansionsDTO, construct_naampad
from pathlib import Path
from API.Enums import AuthType, Environment
from Generic.ExcelModifier import ExcelModifier


if __name__ == '__main__':
    # TODO: in notebook met de cookie werken, zie voorbeeld SNGW ServiceNowGateway
    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    edelta_dossiernummer = 'VWT/DVM/2023/3'
    print(f'De mogelijke document categoriën zijn: {[item.value for item in DocumentCategorieEnum]}')
    document_categorien = [DocumentCategorieEnum.KEURINGSVERSLAG, DocumentCategorieEnum.ELEKTRISCH_SCHEMA]
    print(f'Ophalen van alle documenten van het type {document_categorien}, voor dossiernummer {edelta_dossiernummer}.')

    # Get assets (incl. parents) based on bestek_ref_uuid
    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(edelta_dossiernummer)
    bestek_ref_uuid = bestek_ref[0].uuid
    # TODO dit kan in een functie in em_infra_client
    # TODO query_dto uitbreiden zodat enkel de actieve assets worden opgehaald. Default functie: search_assets.
    #  Extra functie: search_all_assets (die ook de inactieve ophaalt).

    query_dto_search_assets = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='actiefBestek',
                                                operator=OperatorEnum.EQ,
                                                value=f'{bestek_ref_uuid}')])]
                         )
                         , expansions=ExpansionsDTO(fields=["parent"])
                         )
    # Include a maximum number of assets during development
    # max_assets = 5
    # asset_bucket = list(islice(eminfra_client.search_assets(query_dto_search_assets), max_assets))
    # TODO de List hier weglaten
    asset_bucket_generator = eminfra_client.search_assets(query_dto_search_assets)

    # Store assets in a pandas dataframe
    df_assets = pd.DataFrame(columns=["uuid", "assettype", "naam", "naampad", "actief", "toestand", "gemeente", "provincie", "document_categorie", "document_naam", "document_uuid"])
    # build query to download documents
    querys_dto_search_documents = []
    # TODO wijzig de query_search_document naar IN operator met de lijst
    for document_categorie in document_categorien:
        querys_dto_search_documents.append(
            QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                     selection=SelectionDTO(
                         expressions=[ExpressionDTO(
                             terms=[TermDTO(property='categorie',
                                            operator=OperatorEnum.EQ,
                                            value=document_categorie)])]))
        )

    # Create temp folder, download all .pdf-files, write overview and zip all results to an output folder (Downloads).
    downloads_path = Path.home() / "Downloads"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # TODO vervang de progress bar door de library "tqdm", zie voorbeeld in de modelbuilder.
        print("Downloading documents...")
        for i, asset in enumerate(asset_bucket_generator):
            # len_asset_bucket = len(asset_bucket)
            # Check if progress reaches a new tenth percentage
            # if len_asset_bucket == 0:
            #     print("No elements to process.")
            # else:
            #     step = max(1, len_asset_bucket // 10)  # Ensure step is at least 1
            #     # Check if progress reaches a new tenth percentage
            #     if i % step == 0 or i == len_asset_bucket:
            #         percentage = (i * 100) // len_asset_bucket
            #         print(f"Processed {percentage}%")

            # documents = eminfra_client.search_documents_by_asset_uuid(asset_uuid=asset.uuid, query_dto=query_dto_search_documents)
            for query_dto_search_documents in querys_dto_search_documents:
                documents = eminfra_client.search_documents_by_asset_uuid(asset_uuid=asset.uuid, query_dto=query_dto_search_documents)
                for document in documents:
                    # Creates a new record for each unique combination: asset and document
                    row = len(df_assets)
                    # TODO optimaliseren: eerst een dictionary aanmaken en dan de volledige rij toevoegen aan het df.
                    # meerdere rijen per asset toevoegen.
                    df_assets.loc[row,  "uuid"] = asset.uuid
                    df_assets.loc[row, "assettype"] = asset.type.afkorting
                    df_assets.loc[row, "naam"] = asset.naam
                    df_assets.loc[row, "naampad"] = construct_naampad(asset)
                    df_assets.loc[row, "actief"] = asset.actief
                    df_assets.loc[row, "toestand"] = asset.toestand.value
                    locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset.uuid)
                    if locatiekenmerk.locatie:  # Skip when locatiekenmerk is None
                        locatie_adres = locatiekenmerk.locatie.get('adres')
                        if locatie_adres:
                            df_assets.loc[row, "gemeente"] = locatie_adres.get('gemeente')
                            df_assets.loc[row, "provincie"] = locatie_adres.get('provincie')
                    # Append document info to the pandas dataframe
                    df_assets.loc[row, "document_categorie"] = document.categorie.value
                    df_assets.loc[row, "document_naam"] = document.naam
                    df_assets.loc[row, "document_uuid"] = document.uuid

                    # df_assets.loc[df_assets["uuid"] == asset.uuid, "document_categorie"] = document.categorie.value
                    # df_assets.loc[df_assets["uuid"] == asset.uuid, "document_naam"] = document.naam
                    # df_assets.loc[df_assets["uuid"] == asset.uuid, "document_uuid"] = document.uuid

                    # Write document to temp_dir
                    eminfra_client.download_document(document=document, directory=temp_path / asset.uuid / document.categorie.value)

        # Write overview to temp_dir
        edelta_dossiernummer_str = re.sub('[^0-9a-zA-Z]+', '_', edelta_dossiernummer) # replace all non-alphanumeric characters with an underscore
        output_file_path_excel = temp_path / 'overzicht.xlsx'
        df_assets.to_excel(excel_writer=output_file_path_excel
                           , sheet_name=edelta_dossiernummer_str
                           , index=False
                           , engine="openpyxl")

        excelModifier = ExcelModifier(output_file_path_excel)
        excelModifier.add_hyperlink(sheet=edelta_dossiernummer_str, link_type='eminfra', env=Environment.PRD)

        # Zip the output and remove the temp folder
        zip_path = downloads_path / f'documenten_{edelta_dossiernummer_str}'  # The output path of the zip file (without extension)
        shutil.make_archive(str(zip_path), 'zip', root_dir=str(temp_dir))
        print(f"Folder {temp_dir} has been zipped to {zip_path}.zip.")