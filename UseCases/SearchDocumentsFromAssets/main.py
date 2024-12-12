import shutil
from itertools import islice
import pandas as pd
import re

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import DocumentCategorieEnum, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, \
    OperatorEnum, ExpansionsDTO
from pathlib import Path
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # Stap 0. initialiseer dossiernummer en categorie
    edelta_dossiernummer = 'VWT/DVM/2023/3'
    categorie = DocumentCategorieEnum.KEURINGSVERSLAG

    # Stap 1. bestek_ref op basis van dossiernummer
    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(edelta_dossiernummer)
    print(f'bestek_ref: {bestek_ref}')
    bestek_ref_uuid = bestek_ref[0].uuid
    print(f'bestek_ref_uuid: {bestek_ref_uuid}')

    # Stap 2. assets op basis van bestek_ref
    query_dto_search_assets = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='actiefBestek',
                                                operator=OperatorEnum.EQ,
                                                value=f'{bestek_ref_uuid}')])]
                         )
                         , expansions=ExpansionsDTO(fields=["parent"])
                         )

    # Store at max X assets in a list.
    # max_assets = 10
    # asset_bucket = list(islice(eminfra_client.search_assets(query_dto_search_assets), max_assets))
    asset_bucket = list(eminfra_client.search_assets(query_dto_search_assets))

    # Store assets in a pandas dataframe:
    df_assets = pd.DataFrame(columns=["asset_uuid", "document_categorie", "document_naam", "document_uuid"])
    df_assets["asset_uuid"] = [asset.uuid for asset in asset_bucket]

    # Stap 3. documenten op basis van assets
    document_bucket = set()
    query_dto_search_documents = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='categorie',
                                                operator=OperatorEnum.EQ,
                                                value=categorie)])]))
    for asset in asset_bucket:
        # print(f'Opzoeking van de documenten van het type "{categorie}" voor de asset met als uuid: "{asset.uuid}"')
        documents = eminfra_client.search_documents_by_asset_uuid(asset_uuid=asset.uuid, query_dto=query_dto_search_documents)
        # documents = eminfra_client.get_documents_by_asset_uuid(asset_uuid=asset.uuid)
        for document in documents:
            # Append document object to the document bucket
            document_bucket.add(document)
            # Append document info to the pandas dataframe
            df_assets.loc[df_assets["asset_uuid"] == asset.uuid, "document_categorie"] = document.categorie.value
            df_assets.loc[df_assets["asset_uuid"] == asset.uuid, "document_naam"] = document.naam
            df_assets.loc[df_assets["asset_uuid"] == asset.uuid, "document_uuid"] = document.uuid

    # Stap 4. Bewaar de documenten en het overzicht in een Excel-tabel.
    # Zip alle resultaten.
    with Path(r"C:\Users\DriesVerdoodtNordend\Downloads\tmp") as temp_dir:
    # with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir.mkdir(exist_ok=True)

        # Download each individual file in the temporary directory
        for document_uuid in df_assets[df_assets.loc[:, "document_uuid"].notna()].loc[:, "document_uuid"].tolist():
            #eminfra_client.download_document(document_uuid=document.uuid, directory=temp_dir)
            print(f'document_uuid: {document_uuid}')

        # Write the dataframe to Excel. (overview)
        edelta_dossiernummer_str = re.sub('[^0-9a-zA-Z]+', '_', edelta_dossiernummer) # replace all non-alphanumeric characters with an underscore
        df_assets.to_excel(excel_writer=temp_dir/f'{edelta_dossiernummer_str}_overzicht_{categorie.value}.xlsx'
                           , sheet_name=edelta_dossiernummer_str
                           , index=False)

        zip_path = temp_dir.parent / 'output'  # The full path of the zip file (without extension)
        shutil.make_archive(str(zip_path), 'zip', root_dir=str(temp_dir))
        shutil.rmtree(temp_dir)
        print(f"Folder {temp_dir} has been zipped to {zip_path}.zip and the temporary folder has been removed.")
