import logging
import os.path
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import OperatorEnum, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, TermDTO, \
    ExpressionDTO, LogicalOpEnum

from UseCases.utils import load_settings_path, configure_logger

ROOT_FOLDER = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets'
INPUT_FILE = os.path.join(ROOT_FOLDER, 'Report0221', 'Report0221.xlsx')


def initiate_row() -> dict:
    """
    Returns a row dictionary
    """
    return {
        "kast.uuid": None,
        "kast.name": None,
        "kast.assettype": None,
        "kast.locatie.geometrie": None,
        "wv.uuid": None,
        "wv.name": None,
        "wv.assettype": None,
        "wv.locatie.geometrie": None,
        "vvop.uuid": None,
        "vvop.name": None,
        "vvop.assettype": None,
        "vvop.locatie.geometrie": None
    }

def initiate_query_dto(installatie_naam: str) -> QueryDTO:
    """
    Initiate a QueryDTO object to search for a Kast (Legacy) that starts with a certain naampad.
    """
    assettype_kast = "10377658-776f-4c21-a294-6c740b9f655e"
    return QueryDTO(
        size=10,
        from_=0,
        pagingMode=PagingModeEnum.OFFSET,
        expansions=ExpansionsDTO(fields=['parent']),
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=assettype_kast)]),
                ExpressionDTO(
                    terms=[TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH, value=installatie.naam)],
                    logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(property='beheerobject', operator=OperatorEnum.EQ, value=None)],
                    logicalOp=LogicalOpEnum.AND)
            ]
        )
    )


def update_row_info(row: dict, key: str, value: str) -> dict:
    """
    Update dictionaty with key-value combination. Execute check on existing keys and update its value.
    """
    if key not in row.keys():
        raise ValueError(f"Key {key} is not present in row")
    dict[key] = value
    return dict


if __name__ == '__main__':
    configure_logger()
    logging.info('https://github.com/davidvlaminck/AWVGeneric/issues/190')
    settings_path = load_settings_path(user='Dries')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = pd.read_excel(INPUT_FILE, sheet_name='JSONFeature', header=0, usecols=["uuid", "naam", "naampad"])
    rows = []
    row = initiate_row()
    for idx, df_row in df_assets.iterrows():
        asset_uuid = df_row["uuid"]
        logging.info(f"Processing asset: ({idx + 1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
        asset = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=asset_uuid)

        logging.info("Search installatie")
        installatie = eminfra_client.asset_service.search_parent_asset_by_uuid(
            asset_uuid=asset_uuid, return_all_parents=False, recursive=True)
        installatie_naam = installatie.naam

        logging.info("Search Kast")
        query_search_kast = initiate_query_dto(installatie_naam=installatie_naam)
        kasten = list(eminfra_client.asset_service.search_assets_generator(query_dto=query_search_kast, actief=True))
        if len(kasten) != 1:
            raise ValueError('Gegeven de criteria, zijn er meerdere Kasten (Legacy) teruggevonden in éénzelfde boomstructuur.')
        kast = kasten[0]

        logging.info("Get location of Kast")
        locatie = eminfra_client.locatie_service.get_locatie_by_uuid(asset_uuid=kast.uuid)

        logging.info("Transfer location to asset")
        logging.info("Update location info")
        eminfra_client.locatie_service.update_locatie_by_uuid(bron_asset_uuid=asset_uuid, wkt_geometry=locatie.geometrie)

        row = update_row_info(row=row, key='kast.uuid', value=kast.uuid)
        row = update_row_info(row=row, key='kast.naam', value=kast.naam)
        row = update_row_info(row=row, key='kast.assettype', value=kast.type.uri)
        row = update_row_info(row=row, key='kast.locatie.geometrie', value=locatie.geometrie)
        row = update_row_info(row=row, key='wv.uuid', value=asset.uuid)
        row = update_row_info(row=row, key='wv.naam', value=asset.naam)
        row = update_row_info(row=row, key='wv.assettype', value=asset.type.uri)
        row = update_row_info(row=row, key='wv.locatie.geometrie', value=locatie.geometrie)
        row = update_row_info(row=row, key='vvop.uuid', value=asset.uuid)
        row = update_row_info(row=row, key='vvop.naam', value=asset.naam)
        row = update_row_info(row=row, key='vvop.assettype', value=asset.type.uri)
        row = update_row_info(row=row, key='vvop.locatie.geometrie', value=locatie.geometrie)

        rows.append(row)

    output_excel_path = 'test_output.xlsx'
    # Append to existing file
    with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 1))
    # Write to a new file
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 1))