import copy
import logging
import tempfile
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import DocumentCategorieEnum

from UseCases.utils import load_settings_path, configure_logger

WORKING_DIR = Path(r"C:\Users\DriesVerdoodtNordend\OneDrive - Vlaamse overheid - Office 365\1_AWVGeneric\195_documenten_overdragen_lsdeel_naar_kast")
INPUT_FILE = WORKING_DIR / 'input' / 'Documenten_LSDeel_naar_Kast_20260309.xlsx'
OUTPUT_FILE = WORKING_DIR / 'output' / 'Documenten overgezet van LSDeel naar Kast 20260309.xlsx'
ENVIRONMENT = Environment.PRD
TEMP_DIR = Path(tempfile.TemporaryDirectory().name)

def initiate_row() -> dict:
    """
    Initiate a row for the output Excel file.
    """
    return {
        "lsdeel.uuid": None,
        "lsdeel.naam": None,
        "kast.uuid": None,
        "kast.naam": None,
        "document.naam": None,
        "document.categorie": None,
        "opmerking": None
    }

if __name__ == '__main__':
    configure_logger()
    logging.info('Overdagen documenten van LSDeel naar Kast.')
    logging.info('Document types naargelang beschikbaarheid in het input document.')
    eminfra_client = EMInfraClient(env=ENVIRONMENT, auth_type=AuthType.JWT, settings_path=load_settings_path())

    logging.info('Inlezen alle documenten in een pandas dataframe.')
    excel_cols = ['kast.uuid', 'kast.naam', 'lsdeel.uuid', 'lsdeel.naam', 'document.naam', 'document.categorie']
    df = pd.read_excel(INPUT_FILE, sheet_name='Sheet1', header=0, usecols=excel_cols)

    rows = []
    counter = 0
    for index, df_row in df.iterrows():
        row = initiate_row()
        asset = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=df_row["lsdeel.uuid"])
        if asset is None:
            break
        counter += 1
        logging.info(f"Processing asset: ({counter}); asset_uuid: {asset.uuid}; asset_name: {asset.naam}")
        row["lsdeel.uuid"] = asset.uuid
        row["lsdeel.naam"] = asset.naam

        logging.info('Search documents.')
        categorie = DocumentCategorieEnum(df_row["document.categorie"])
        documents = eminfra_client.document_service.get_documents_by_uuid_generator(asset_uuid=asset.uuid, size=100,
                                                                                    categorie=[categorie])
        documents_list = list(documents)
        if len(documents_list) == 0:
            log_message = 'Geen documenten teruggevonden.'
            logging.info(log_message)
            row["opmerking"] = log_message
            rows.append(row)
        else:
            logging.info('Move document(s) from LSDeel to Kast')
            kast = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=df_row["kast.uuid"])
            row["kast.uuid"] = kast.uuid
            row["kast.naam"] = kast.naam

            for doc in documents_list:
                row = copy.deepcopy(row)
                logging.info('upload document to Kast')
                temp_path_document = eminfra_client.document_service.download_document(document=doc, directory=TEMP_DIR)
                eminfra_client.document_service.upload_document(asset_uuid=kast.uuid,
                                                                file_path=temp_path_document,
                                                                omschrijving=doc.omschrijving,
                                                                documentcategorie=doc.categorie)

                logging.info('Remove document from LSDeel')
                eminfra_client.document_service.remove_document(asset_uuid=asset.uuid, document=doc)
                row["document.naam"] = doc.naam
                row["document.categorie"] = doc.categorie.value
                row["opmerking"] = 'Document verplaatst van LSDeel naar Kast'

                rows.append(row)

    # Append to existing file
    if OUTPUT_FILE.exists():
        with pd.ExcelWriter(OUTPUT_FILE, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 4))
    else:
        # Write to a new file
        with pd.ExcelWriter(OUTPUT_FILE, mode='w', engine='openpyxl') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 4))