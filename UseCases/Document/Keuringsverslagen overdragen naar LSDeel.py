import copy
import logging
import tempfile
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import DocumentCategorieEnum

from UseCases.utils import load_settings_path, configure_logger, filter_asset_type
from utils.query_dto_helpers import build_query_search_assettype

OUTPUT_DIR = Path(r"C:\Users\DriesVerdoodtNordend\OneDrive - Vlaamse overheid - Office 365\python_repositories\AWVGeneric\UseCases\Document")
ASSETTYPE_KAST = '10377658-776f-4c21-a294-6c740b9f655e'
# DOCUMENT_CATEGORIEN = [DocumentCategorieEnum.ELEKTRISCH_SCHEMA, DocumentCategorieEnum.KEURINGSVERSLAG]
DOCUMENT_CATEGORIEN = None
ENVIRONMENT = Environment.PRD
TEMP_DIR = Path(tempfile.TemporaryDirectory().name)

def initiate_row() -> dict:
    """
    Initiate a row for the output excel file.
    """
    return {
        "kast.uuid": None,
        "kast.naam": None,
        "lsdeel.uuid": None,
        "lsdeel.naam": None,
        "document.naam": None,
        "document.categorie": None,
        "opmerking": None
    }

if __name__ == '__main__':
    configure_logger()
    logging.info('Overdagen documenten van Kast naar onderliggende LSDeel.')
    logging.info('Document types: Keuringsverslag, Elektrisch schema')
    eminfra_client = EMInfraClient(env=ENVIRONMENT, auth_type=AuthType.JWT, settings_path=load_settings_path())

    query_dto = build_query_search_assettype(assettype_uuid=ASSETTYPE_KAST)
    generator_assets = eminfra_client.asset_service.search_assets_generator(query_dto=query_dto)

    rows = []
    counter = 0
    while True:
        row = initiate_row()
        asset = next(generator_assets, None)  # None is returned when generator is exhausted
        if asset is None:
            break
        counter += 1
        logging.info(f"Processing asset: ({counter}); asset_uuid: {asset.uuid}; asset_name: {asset.naam}")
        row["kast.uuid"] = asset.uuid
        row["kast.naam"] = asset.naam

        logging.info('Search documents.')
        documents = eminfra_client.document_service.get_documents_by_uuid_generator(asset_uuid=asset.uuid, size=100,
                                                                                    categorie=DOCUMENT_CATEGORIEN)
        documents_list = list(documents)
        if len(documents_list) == 0:
            log_message = 'Geen documenten onder de Kast.'
            logging.info(log_message)
            row["opmerking"] = log_message
            rows.append(row)
        else:
            logging.info('Search child-asset LSDeel')
            child_asset_generator = eminfra_client.asset_service.search_child_assets_by_uuid_generator(asset_uuid=asset.uuid, recursive=True)
            list_lsdeel = filter_asset_type(assets = list(child_asset_generator), uri='https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel')
            if len(list_lsdeel) == 0:
                log_message = 'Geen LSDeel onder Kast.'
                logging.warning(log_message)
                row["opmerking"] = log_message
                rows.append(row)

            elif len(list_lsdeel) > 1:
                log_message = 'Meerdere LSDeel onder Kast.'
                logging.info(log_message)
                row["opmerking"] = log_message
                rows.append(row)

            else:
                lsdeel = list_lsdeel[0]
                row["lsdeel.uuid"] = lsdeel.uuid
                row["lsdeel.naam"] = lsdeel.naam

                logging.info('Move document to child-asset LSDeel')
                for doc in documents_list:
                    row = copy.deepcopy(row)
                    logging.info('upload document to LSDeel')
                    # 20.02.2026: internal server error op de TEI omgeving.
                    temp_path_document = eminfra_client.document_service.download_document(document=doc, directory=TEMP_DIR)
                    eminfra_client.document_service.upload_document(asset_uuid=lsdeel.uuid, file_path=temp_path_document, omschrijving=doc.omschrijving, documentcategorie=doc.categorie)

                    logging.info('Remove document from Kast')
                    eminfra_client.document_service.remove_document(asset_uuid=asset.uuid, document=doc)
                    row["document.naam"] = doc.naam
                    row["document.categorie"] = doc.categorie.value
                    row["opmerking"] = 'Document verplaatst van Kast naar LSDeel'

                    rows.append(row)

        # todo remove statement to break out of the Kast-generator
        # if counter % 1000 == 0:
        #     break

    output_excel_path = OUTPUT_DIR /  f'Keuringsverslagen_Kast_naar_LSDeel_{ENVIRONMENT.name}.xlsx'
    # Append to existing file
    if output_excel_path.exists():
        with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 4))
    else:
        # Write to a new file
        with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 4))