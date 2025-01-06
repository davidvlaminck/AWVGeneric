import itertools
import shutil
import tempfile
from datetime import datetime
from itertools import islice

import pandas as pd
import re


from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import DocumentCategorieEnum, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, \
    OperatorEnum, ExpansionsDTO, construct_naampad, LogicalOpEnum
from pathlib import Path
from API.Enums import AuthType, Environment
from Generic.ExcelModifier import ExcelModifier

if __name__ == '__main__':
    # TODO: in notebook met de cookie werken, zie voorbeeld SNGW ServiceNowGateway
    settings_path = Path(
        'C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    edelta_dossiernummer = 'VWT/DVM/2023/3'
    print(f'De mogelijke document categoriën zijn: {[item.value for item in DocumentCategorieEnum]}')
    document_categorien = [DocumentCategorieEnum.KEURINGSVERSLAG, DocumentCategorieEnum.ELEKTRISCH_SCHEMA]
    print(f'Ophalen van alle documenten van het type {document_categorien}, voor dossiernummer {edelta_dossiernummer}.')

    # Get assets (incl. parents) based on bestek_ref_uuid
    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(edelta_dossiernummer)
    bestek_ref_uuid = bestek_ref[0].uuid

    # build query to search assets within a certain order (bestek)
    query_dto_search_assets = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                       selection=SelectionDTO(
                                           expressions=[
                                               ExpressionDTO(
                                                   terms=[TermDTO(property='actiefBestek',
                                                                  operator=OperatorEnum.EQ,
                                                                  value=f'{bestek_ref_uuid}')
                                                          ]
                                                   , logicalOp=None)
                                           ]
                                       )
                                       , expansions=ExpansionsDTO(fields=["parent"])
                                       )

    # build query to download documents
    query_dto_search_document = QueryDTO(
        size=100
        , from_=0
        , pagingMode=PagingModeEnum.OFFSET
        , selection=SelectionDTO(
            expressions=[ExpressionDTO(
                terms=[TermDTO(property='categorie',
                               operator=OperatorEnum.IN,
                               value=[categorie.value for categorie in document_categorien])]
            )]
        )
    )

    # Create temp folder, download all .pdf-files, write overview and zip all results to an output folder (Downloads).
    downloads_path = Path.home() / "Downloads"
    print("Downloading documents...")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        asset_bucket_generator = eminfra_client.search_assets(query_dto_search_assets)

        # Store assets in a pandas dataframe
        df_assets = pd.DataFrame(
            columns=["uuid", "assettype", "naam", "naampad", "actief", "toestand", "gemeente", "provincie",
                     "document_categorie", "document_naam", "document_uuid"])

        # TODO de List hier weglaten na development
        max_assets = 20 # include a maximum number of X assets during development
        start_time = datetime.now()
        # for i, asset in enumerate(list(islice(asset_bucket_generator, max_assets)), start=1):
        for i, asset in enumerate(asset_bucket_generator):
            # Track progress
            if i % 10 == 0:
                elapsed = datetime.now() - start_time
                print(f"Processed {i} assets in {elapsed.total_seconds():.2f} seconds")

            documents = eminfra_client.search_documents_by_asset_uuid(
                asset_uuid=asset.uuid
                , query_dto=query_dto_search_document
            )
            for document in documents:
                # Voorbereiding variabelen: gemeente, provincie, naampad
                locatiekenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset.uuid)
                if locatiekenmerk.locatie.get('adres'):  # Skip when locatiekenmerk.locatie is None
                    locatie_adres = locatiekenmerk.locatie.get('adres')
                    gemeente = locatie_adres.get('gemeente')
                    provincie = locatie_adres.get('provincie')
                naampad = construct_naampad(asset)

                # Eigenschappen in een dictionary plaatsen en de dictionary toevoegen aan een dataframe
                row_dict = {
                    'uuid': asset.uuid
                    , 'assettype': asset.type.afkorting
                    , 'naam': asset.naam
                    , 'naampad': naampad
                    , 'actief': asset.actief
                    , 'toestand': asset.toestand.value
                    , 'gemeente': gemeente
                    , 'provincie': provincie
                    , 'document_categorie': document.categorie.value
                    , 'document_naam': document.naam
                    , 'document_uuid': document.uuid
                }
                row_df = pd.DataFrame([row_dict])
                df_assets = pd.concat([df_assets, row_df], ignore_index=True)

                # Write document to temp_dir
                eminfra_client.download_document(
                    document=document
                    , directory=temp_path / naampad.replace('/', '__') / document.categorie.value
                )

        # Write overview to temp_dir
        edelta_dossiernummer_str = re.sub('[^0-9a-zA-Z]+', '_',
                                          edelta_dossiernummer)  # replace all non-alphanumeric characters with an underscore
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