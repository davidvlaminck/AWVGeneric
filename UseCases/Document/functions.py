import shutil
import tempfile
from datetime import datetime

import pandas as pd
import re

from API.eminfra.EMInfraDomain import DocumentCategorieEnum, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, \
    OperatorEnum, ExpansionsDTO, construct_naampad, LogicalOpEnum, ApplicationEnum, ProvincieEnum
from API.Enums import Environment
from pathlib import Path
from Generic.ExcelModifier import ExcelModifier

def download_documents(eminfra_client, edelta_dossiernummer: str, document_categorie:[DocumentCategorieEnum] = None, toezichter:str = None, provincie:[ProvincieEnum] = None) -> Path|None:
    """Download documents

    Downloading documents from EM-Infra based on the criteria:
        edelta_dossiernummer, document_categorien, toezichter (optional), provincie (optional)

    :param eminfra_client: eminfra Client voor de authenticatie met EM-Infra
    :param edelta_dossiernummer: str Dossiernummer
    :param document_categorie: [DocumentCategorieEnum] Lijst van Document categoriën
    :param toezichter: str Naam van de toezichter
    :param provincie: [ProvincieEnum] Lijst van provincienamen
    :returns Path of the .zip-file where all the results have been downloaded.
    :rtype: Path
    """
    provincie_value = [item.value for item in provincie] if provincie else provincie
    if document_categorie:
        document_categorie_value = [item.value for item in document_categorie]
    else:
        # if None, use all the enumeration values.
        document_categorie_value = [item.value for item in DocumentCategorieEnum]

    print(f'Ophalen van alle documenten die voldoen aan volgende criteria:'
          f'\tDocument categorie: {document_categorie_value}'
          f'\tDossiernummer: {edelta_dossiernummer}'
          f'\tToezichter: {toezichter}'
          f'\tProvincie: {provincie_value}')

    bestek_ref = eminfra_client.get_bestekref_by_eDelta_dossiernummer(edelta_dossiernummer)
    bestek_ref_uuid = bestek_ref.uuid

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
                terms=[TermDTO(property='categorie', operator=OperatorEnum.IN, value=document_categorie_value)]
            )]
        )
    )

    # Create temporary folder, download all files, write an overview in an Excel file and zip all results to an output folder (Downloads).
    downloads_path = Path.home() / 'Downloads' / 'Results'
    print("Downloading documents...")

    with (tempfile.TemporaryDirectory() as temp_dir):
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
            locatiekenmerk = eminfra_client.get_locatie(asset.uuid)
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

            documents = list(
                eminfra_client.search_documents_by_asset_uuid(
                    asset_uuid=asset.uuid
                    , query_dto=query_dto_search_document
                )
            )

            # create a folder in the temp path
            directory_path = temp_path / locatie_provincie / toezichter_volledige_naam

            if documents:
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
                        , directory = directory_path / naampad.replace('/', '__') / document.categorie.value
                    )
            else:
                # when documents are missing, append prefix "geen_documenten_"to the folder name.
                directory_path_geen_bestanden_beschikbaar = directory_path / '_geen_bestanden_beschikbaar' / naampad.replace('/', '__')
                directory_path_geen_bestanden_beschikbaar.mkdir(parents=True, exist_ok=True)
                print(f"Directory '{directory_path_geen_bestanden_beschikbaar}' is created or already exists.")

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
        zip_path = downloads_path / f'documenten_{edelta_dossiernummer_str}'
        shutil.make_archive(str(zip_path), 'zip', root_dir=str(temp_dir))
        print(f"Folder {temp_dir} has been zipped to {zip_path}.")

        return zip_path
