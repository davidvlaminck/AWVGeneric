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
from Generic.ExcelEditor import ExcelEditor

if __name__ == '__main__':
    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    edelta_dossiernummer = 'VWT/DVM/2023/3'
    categorie = DocumentCategorieEnum.KEURINGSVERSLAG
    print(f'Ophalen van alle documenten van het type {categorie}, voor dossiernummer {edelta_dossiernummer}.')

    # Get assets (incl. parents) based on bestek_ref_uuid
    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(edelta_dossiernummer)
    bestek_ref_uuid = bestek_ref[0].uuid
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
    asset_bucket = list(eminfra_client.search_assets(query_dto_search_assets))

    # Store assets in a pandas dataframe
    df_assets = pd.DataFrame(columns=["uuid", "assettype", "naam", "naampad", "actief", "toestand", "gemeente", "provincie", "document_categorie", "document_naam", "document_uuid"])
    for i, asset in enumerate(asset_bucket):
        df_assets.loc[i, "uuid"] = asset.uuid
        df_assets.loc[i, "assettype"] = asset.type.afkorting
        df_assets.loc[i, "naam"] = asset.naam
        df_assets.loc[i, "naampad"] = construct_naampad(asset)
        df_assets.loc[i, "actief"] = asset.actief
        df_assets.loc[i, "toestand"] = asset.toestand.value
        locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset.uuid)
        if locatiekenmerk.locatie: # Skip when locatiekenmerk is None
            locatie_adres = locatiekenmerk.locatie.get('adres')
            if locatie_adres:
                df_assets.loc[i, "gemeente"] = locatie_adres.get('gemeente')
                df_assets.loc[i, "provincie"] = locatie_adres.get('provincie')

    # build query to download documents
    query_dto_search_documents = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='categorie',
                                                operator=OperatorEnum.EQ,
                                                value=categorie)])]))

    # Create temp folder, download all .pdf-files, write overview and zip all results to an output folder (Downloads).
    downloads_path = Path.home() / "Downloads"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        print("Downloading documents...")
        len_asset_bucket = len(asset_bucket)
        for i, asset in enumerate(asset_bucket):
            # Check if progress reaches a new tenth percentage
            if len_asset_bucket == 0:
                print("No elements to process.")
            else:
                step = max(1, len_asset_bucket // 10)  # Ensure step is at least 1
                # Check if progress reaches a new tenth percentage
                if i % step == 0 or i == len_asset_bucket:
                    percentage = (i * 100) // len_asset_bucket
                    print(f"Processed {percentage}%")

            documents = eminfra_client.search_documents_by_asset_uuid(asset_uuid=asset.uuid, query_dto=query_dto_search_documents)
            for document in documents:
                # Append document info to the pandas dataframe
                df_assets.loc[df_assets["uuid"] == asset.uuid, "document_categorie"] = document.categorie.value
                df_assets.loc[df_assets["uuid"] == asset.uuid, "document_naam"] = document.naam
                df_assets.loc[df_assets["uuid"] == asset.uuid, "document_uuid"] = document.uuid

                # Write document to temp_dir
                eminfra_client.download_document(document=document, directory=temp_path)

        # Write overview to temp_dir
        edelta_dossiernummer_str = re.sub('[^0-9a-zA-Z]+', '_', edelta_dossiernummer) # replace all non-alphanumeric characters with an underscore
        output_file_path_excel = temp_path / f'{edelta_dossiernummer_str}_overzicht_{categorie.value}.xlsx'
        df_assets.to_excel(excel_writer=output_file_path_excel
                           , sheet_name=edelta_dossiernummer_str
                           , index=False
                           , engine="openpyxl")

        excelEditor = ExcelEditor(output_file_path_excel)
        excelEditor.convert_uuid_to_formula(sheet=edelta_dossiernummer_str, link_type='eminfra', env=Environment.PRD)

        # Zip the output and remove the temp folder
        zip_path = downloads_path / 'output'  # The output path of the zip file (without extension)
        shutil.make_archive(str(zip_path), 'zip', root_dir=str(temp_dir))
        print(f"Folder {temp_dir} has been zipped to {zip_path}.zip.")