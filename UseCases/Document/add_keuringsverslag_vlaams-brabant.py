import logging
from pathlib import Path
import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import QueryDTO, ExpansionsDTO, PagingModeEnum, SelectionDTO, TermDTO, OperatorEnum, \
    ExpressionDTO, LogicalOpEnum, AssetDTO, DocumentCategorieEnum

from UseCases.utils import load_settings, read_rsa_report, configure_logger


def filter_assettype(assets: list[AssetDTO], uri: str):
    """
    Filter assets based on assettype in a list of assettypes
    Returns all the assets of a specific type
    :param child_assets: list of assets
    :type child_assets: list[AssetDTO]
    :param uri: URI of an assttype
    :type uri: str
    return List(AssetDTO)
    """
    return [i for i in assets if i.type.uri == uri]


if __name__ == '__main__':
    configure_logger()
    logging.info('Toevoegen keuringsverslagen:\t'
                 'Keuringsverslagen uit Vlaams-Brabant uploaden bij het LSDeel van de gelijknamige kast.')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    root_folder = Path.home() / 'Nordend' / 'AWV - Documents' / 'Keuringsverslagen' / 'Vlaams-Brabant'
    if not Path.exists(root_folder):
        raise FileExistsError(f'Root folder does not exists: {root_folder}.')
    for f in root_folder.iterdir():
        if f.is_file():
            logging.debug(f'Processing file: {f.name}')
            # search asset
            asset_name = f.stem
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

                # search LSDeel
                child_assets = list(eminfra_client.asset_service.search_child_assets_by_uuid_generator(
                    asset_uuid=kast.uuid, recursive=False))

                assets_lsdeel = filter_assettype(
                    assets=child_assets, uri="https://lgc.data.wegenenverkeer.be/ns/installatie#LSDeel")

#                if len(assets_lsdeel) != 1:
#                    raise ValueError(f'Multiple LSDeel found in Kast: {asset_name}')
#                else:
                asset_lsdeel = assets_lsdeel[0]
                logging.debug('Upload Keuringsverslag to LSDeel')
                eminfra_client.document_service.upload_document(asset_uuid=asset_lsdeel.uuid, file_path=f,
                                                                document_type=DocumentCategorieEnum.KEURINGSVERSLAG,
                                                                omschrijving='test')

            else:
                logging.debug(f'Kast nog found.')


    # df_assets = read_rsa_report()
    #
    # rows = []
    # for idx, df_row in df_assets.iterrows():
    #     asset_uuid = df_row["id"]
    #     logging.info(f"Processing asset: ({idx + 1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
    #     asset = eminfra_client.get_asset_by_id(asset_id=asset_uuid)
    #     row = {
    #         "uuid": ''
    #     }
    #     rows.append(row)
    #
    # output_excel_path = 'test_output.xlsx'
    # # Append to existing file
    # with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    #     df = pd.DataFrame(rows)
    #     df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])
    # # Write to a new file
    # with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
    #     df = pd.DataFrame(rows)
    #     df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])