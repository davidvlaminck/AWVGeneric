import logging
from pathlib import Path
import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import QueryDTO, ExpansionsDTO, PagingModeEnum, SelectionDTO, TermDTO, OperatorEnum, \
    ExpressionDTO, LogicalOpEnum, AssetDTO, DocumentCategorieEnum

from UseCases.utils import load_settings, configure_logger


def filter_assettype(assets: list[AssetDTO], uri: str) -> AssetDTO | None:
    """
    Filter assets based on assettype
    Returns 1 asset of a specific type
    :param assets: list of assets
    :type assets: list[AssetDTO]
    :param uri: URI of an assettype
    :type uri: str
    return AssetDTO | None
    """
    filtered_assets = [i for i in assets if i.type.uri == uri]
    if len(filtered_assets) != 1:
        logging.critical(f'Het aantal assets in de lijst stemt niet overeen met het verwacht aantal assets.')
        return None
    else:
        asset = filtered_assets[0]
    return asset


if __name__ == '__main__':
    configure_logger()
    logging.info('Toevoegen keuringsverslagen:\t'
                 'Keuringsverslagen uit Vlaams-Brabant uploaden bij het LSDeel van de gelijknamige kast.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    root_folder = Path.home() / 'Nordend' / 'AWV - Documents' / 'Keuringsverslagen' / 'Vlaams-Brabant'
    if not Path.exists(root_folder):
        raise FileExistsError(f'Root folder does not exists: {root_folder}.')

    rows = []
    for f in root_folder.iterdir():
        if f.is_file():
            row = {
                "file": f.name,
                "kast_uuid": '',
                "lsdeel_uuid": '',
                "resultaat": ''
            }
            logging.debug(f'Processing file: {f.name}')
            asset_name = f.stem
            logging.info(f'Search asset (name: {asset_name}; type: Kast).')
            query_dto = QueryDTO(size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 expansions=ExpansionsDTO(fields=['parent']),
                                 selection=SelectionDTO(
                                     expressions=[ExpressionDTO(
                                         terms=[
                                             TermDTO(property='naam', operator=OperatorEnum.EQ, value=asset_name),
                                             TermDTO(property='type', operator=OperatorEnum.EQ,
                                                     value='10377658-776f-4c21-a294-6c740b9f655e',
                                                     logicalOp=LogicalOpEnum.AND)
                                         ])]))
            kast = next(eminfra_client.asset_service.search_assets_generator(query_dto=query_dto, actief=True), None)
            if kast:
                logging.debug(f'Found kast: {kast.uuid}: {kast.naam}')
                row["kast_uuid"] = kast.uuid

                logging.debug('Search LSDeel')
                child_assets = list(eminfra_client.asset_service.search_child_assets_by_uuid_generator(
                    asset_uuid=kast.uuid, recursive=False))

                asset_lsdeel = filter_assettype(
                    assets=child_assets, uri="https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel")
                if not asset_lsdeel:
                    continue
                row["lsdeel_uuid"] = asset_lsdeel.uuid

                logging.debug('Upload Keuringsverslag to LSDeel')
                response_code = eminfra_client.document_service.upload_document(
                    file_path=f, asset_uuid=asset_lsdeel.uuid,
                    documentcategorie=DocumentCategorieEnum.KEURINGSVERSLAG,
                    omschrijving=f'Keuringsverslag: {f.stem}')
                if response_code == 202:
                    row["resultaat"] = 'File uploaded'

            else:
                logging.debug(f'Kast not found.')

            rows.append(row)

    output_excel_path = 'Keuringsverslagen_Vlaams-Brabant.xlsx'
    # Write to a new file
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 1))
