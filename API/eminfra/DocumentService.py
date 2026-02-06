import json
import logging
from pathlib import Path
from collections.abc import Generator
from API.eminfra.EMInfraDomain import AssetDocumentDTO, AssetDTO, DocumentCategorieEnum


class DocumentService:
    def __init__(self, requester):
        self.requester = requester

    def download_document(self, document: AssetDocumentDTO, directory: Path) -> Path:
        """ Downloads document into a directory.

        Args:
            document (AssetDocumentDTO): document object
            directory (Path): Path to the (temporary) directory.

        Returns:
            Path: The full path of the downloaded PDF file.
        """
        # Check if the directory exists, create if not exist
        directory.mkdir(parents=True, exist_ok=True)

        file_name = document.naam
        if not document.document['links']:
            raise ValueError("The 'links' list is empty.")
        doc_link = document.document['links'][0]['href'].split('/eminfra/')[1]
        json_str = self.requester.get(doc_link).content.decode("utf-8")
        json_response = json.loads(json_str)
        doc_download_link = \
            next(l for l in json_response['links'] if l['rel'] == 'download')['href'].split('/eminfra/')[1]
        file = self.requester.get(doc_download_link)

        with open(f'{directory}/{file_name}', 'wb') as f:
            logging.info(f'Writing file {file_name} to temp location: {directory}.')
            f.write(file.content)
            return directory / file_name

    def upload_document(self, asset_uuid: str, file_path: Path, document_type: DocumentCategorieEnum,
                        omschrijving: str) -> None:
        """Uploads document to an asset

        Uploads a single document to an asset, using the asset_uuid, the directory to the document and a document
        category.

        :param asset_uuid:
        :type asset_uuid: str
        :param file_path:
        :type file_path: Path
        :param document_type:
        :type document_type: DocumentCategorieEnum
        :param omschrijving: Omschrijving van het document
        :type omschrijving: str
        :return: None
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        url = 'dms/api/documenten'
        # open file as binary
        # (filename, data, content_type)
        headers = {
            'Accept': '*/*', #'application/pdf',
            'Content-Type': 'multipart/form-data'
            #'Content-Disposition': 'form-data',
            #'name': 'file',
            #'filename': file_path.name
        }
        with file_path.open("rb") as f:
            files = {
                "file": (file_path.name, f, "application/pdf")
            }
            response = self.requester.post(url, files=files)
            logging.debug(f'response: {response.status_code}')
            logging.debug(f'response: {response.text}')

        response_json = response.json()
        return response_json


    def get_documents_by_uuid_generator(self, asset_uuid: str, size: int = 10,
                                        categorie: list[DocumentCategorieEnum] = None) -> Generator[AssetDocumentDTO]:
        """
        Retrieves all AssetDocumentDTO associated with an asset. Optionally: filter by document categories.

        :param asset_uuid: Asset uuid
        :type asset_uuid: str
        :param size: aantal documenten
        :type size: str
        :param categorie: document categoriën
        :type categorie: list[DocumentCategorieEnum]
        :return: Generator[AssetDocumentDTO]
        :rtype:
        """
        _from = 0
        while True:
            url = f"core/api/assets/{asset_uuid}/documenten?from={_from}&pagingMode=OFFSET&size={size}"
            json_dict = self.requester.get(url).json()
            if categorie:
                yield from [AssetDocumentDTO.from_dict(item)
                            for item in json_dict['data'] if item.get("categorie") in [cat.value for cat in categorie]]
            else:
                yield from [AssetDocumentDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            _from = json_dict['from'] + size
            if _from >= dto_list_total:
                break

    def get_documents_generator(self, asset: AssetDTO, size: int = 10,
                                categorie: list[DocumentCategorieEnum] = None) -> Generator[AssetDocumentDTO]:
        """
        Retrieves all AssetDocumentDTO associated with an asset
        :param asset:
        :type asset: AssetDTO
        :param size: aantal documenten
        :type size: str
        :param categorie: document categori�n
        :type categorie: list[DocumentCategorieEnum]
        :return: Generator[AssetDocumentDTO]
        :rtype:
        """
        return self.get_documents_by_uuid_generator(asset_uuid=asset.uuid, size=size, categorie=categorie)
