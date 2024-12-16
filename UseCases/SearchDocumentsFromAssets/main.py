import os
import shutil
import tempfile
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
    max_assets = 5
    asset_bucket = list(islice(eminfra_client.search_assets(query_dto_search_assets), max_assets))
    # asset_bucket = list(eminfra_client.search_assets(query_dto_search_assets))

    # Store assets in a pandas dataframe:
    df_assets = pd.DataFrame(columns=["asset_uuid", "assettype", "naam", "naampad", "actief", "toestand", "gemeente", "provincie", "document_categorie", "document_naam", "document_uuid"])
    df_assets["asset_uuid"] = [asset.uuid for asset in asset_bucket]
    df_assets["assettype"] = [asset.type.afkorting for asset in asset_bucket]
    df_assets["naam"] = [asset.naam for asset in asset_bucket]
    df_assets["actief"] = [asset.actief for asset in asset_bucket]
    df_assets["toestand"] = [asset.toestand.value for asset in asset_bucket]

    # TODO voeg naampad, gemeente, provincie toe.
    # Todo toevoegen kastnummer? >> zie mail Ben Cannaerts
    # Staat de kastnummer ook ergens in de excel of kan deze toegevoegd worden, zodat het duidelijk is welke bestanden bij welke kast horen? Dat is de naam van de map, waaronder alle bijhorende documenten zitten op de drive.
    # De bestanden op de drive zijn we inderdaad enkel geïnteresseerd in de PDF's

    # Stap 3. documenten op basis van assets
    query_dto_search_documents = QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[TermDTO(property='categorie',
                                                operator=OperatorEnum.EQ,
                                                value=categorie)])]))

    # Zip alle resultaten.
    output_dir = Path(r"C:\Users\DriesVerdoodtNordend\Downloads")
    # with Path(r"C:\Users\DriesVerdoodtNordend\Downloads\tmp") as temp_dir:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        os.makedirs(temp_path, exist_ok=True)

        for asset in asset_bucket:
            documents = eminfra_client.search_documents_by_asset_uuid(asset_uuid=asset.uuid, query_dto=query_dto_search_documents)
            for document in documents:
                # Append document info to the pandas dataframe
                df_assets.loc[df_assets["asset_uuid"] == asset.uuid, "document_categorie"] = document.categorie.value
                df_assets.loc[df_assets["asset_uuid"] == asset.uuid, "document_naam"] = document.naam
                df_assets.loc[df_assets["asset_uuid"] == asset.uuid, "document_uuid"] = document.uuid

                # Stap 4. Write documents to temp_dir
                eminfra_client.download_document(document=document, directory=temp_path)


        # Write overview to temp_dir
        edelta_dossiernummer_str = re.sub('[^0-9a-zA-Z]+', '_', edelta_dossiernummer) # replace all non-alphanumeric characters with an underscore
        df_assets.to_excel(excel_writer=temp_path / f'{edelta_dossiernummer_str}_overzicht_{categorie.value}.xlsx'
                           , sheet_name=edelta_dossiernummer_str
                           , index=False)

        # Zip the output and remove the temp folder (redundant step, since we use the with-statement).
        zip_path = output_dir / 'output'  # The output path of the zip file (without extension)
        shutil.make_archive(str(zip_path), 'zip', root_dir=str(temp_dir))
        shutil.rmtree(temp_dir) # this is redunant
        print(f"Folder {temp_dir} has been zipped to {zip_path}.zip and the temporary folder has been removed.")