import logging
import os.path
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import OperatorEnum, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, TermDTO, \
    ExpressionDTO, LogicalOpEnum, LocatieKenmerk

from UseCases.utils import load_settings_path, configure_logger

ROOT_FOLDER = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0221'
INPUT_FILE = os.path.join(ROOT_FOLDER, 'Report0221.xlsx')


def initiate_row() -> dict:
    """
    Returns a row dictionary
    """
    return {
        "kast.uuid": None,
        "kast.naam": None,
        "kast.toestand": None,
        "kast.assettype": None,
        "kast.geometrie": None,
        "wv.uuid": None,
        "wv.naam": None,
        "wv.toestand": None,
        "wv.assettype": None,
        "wv.geometrie": None,
        "opmerking": None
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
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                    terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=assettype_kast)]),
                ExpressionDTO(
                    terms=[TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH, value=installatie_naam)],
                    logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(property='beheerobject', operator=OperatorEnum.EQ, value=None, negate=True)],
                    logicalOp=LogicalOpEnum.AND),
                ExpressionDTO(
                    terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)],
                    logicalOp=LogicalOpEnum.AND)
            ]
        )
    )


def update_row_info(row_info: dict, key: str, value: str) -> dict:
    """
    Update dictionary with key-value combination. Execute check on existing keys and update its value.
    """
    if key not in row.keys():
        raise ValueError(f"Key {key} is not present in row")
    row_info[key] = value
    return row_info


def parse_locatie_kenmerk_2_wkt(locatie_kenmerk: LocatieKenmerk) -> str | None:
    """
    Parse het LocatieKenmerk naar een WKT-string
    """
    locatie = locatie_kenmerk.locatie
    if locatie:
        geometrie = locatie.get("geometrie")
        crs = geometrie.get('crs')
        type = geometrie.get('type')
        coordinates = geometrie.get('coordinates')
        if type == 'Point':
            x = coordinates[0]
            y = coordinates[1]
            if len(coordinates) == 3:
                z = coordinates[2]
            else:
                z = 0
            wkt_geom = f'POINT Z({x} {y} {z})'
        else:
            raise ValueError("Not implemented yet")
    else:
        wkt_geom = None
    return wkt_geom


if __name__ == '__main__':
    configure_logger()
    logging.info('https://github.com/davidvlaminck/AWVGeneric/issues/190')
    settings_path = load_settings_path(user='Dries')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    df_assets = pd.read_excel(INPUT_FILE, sheet_name='JSONFeature', header=0, usecols=["uuid", "naam", "naampad"])
    rows = []
    for idx, df_row in df_assets.iterrows():
        row = initiate_row()
        asset_uuid = df_row["uuid"]
        logging.info(f"Processing wv: ({idx + 1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
        wv = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=asset_uuid)
        row = update_row_info(row_info=row, key='wv.uuid', value=wv.uuid)
        row = update_row_info(row_info=row, key='wv.naam', value=wv.naam)
        row = update_row_info(row_info=row, key='wv.toestand', value=wv.toestand.value)
        row = update_row_info(row_info=row, key='wv.assettype', value=wv.type.uri)

        logging.info("Search installatie")
        installatie = eminfra_client.asset_service.search_parent_asset_by_uuid(
            asset_uuid=asset_uuid, return_all_parents=False, recursive=True)
        if not installatie:
            logging.warning('WV Lichtmast (Legacy) zit niet in een boomstructuur.')
            row = update_row_info(row_info=row, key='opmerking', value='WV (Legacy) staat niet in een boomstructuur')
            rows.append(row)
            continue

        logging.info("Search Kast")
        query_search_kast = initiate_query_dto(installatie_naam=installatie.naam)
        kasten = list(eminfra_client.asset_service.search_assets_generator(query_dto=query_search_kast))
        if len(kasten) != 1:
            log_message = (f'Gegeven de criteria, zijn er {len(kasten)} Kasten (Legacy) teruggevonden in eenzelfde '
                           f'boomstructuur in plaats van 1. Locatie wordt niet overgedragen.')
            logging.warning(log_message)
            row = update_row_info(row_info=row, key='opmerking', value=log_message)

        else:
            logging.info('Found 1 Kast, continue.')
            kast = kasten[0]

            logging.info("Get location of Kast")
            locatie_kenmerk = eminfra_client.locatie_service.get_locatie_by_uuid(asset_uuid=kast.uuid)
            kast_wkt_geometrie = parse_locatie_kenmerk_2_wkt(locatie_kenmerk=locatie_kenmerk)

            row = update_row_info(row_info=row, key='kast.uuid', value=kast.uuid)
            row = update_row_info(row_info=row, key='kast.naam', value=kast.naam)
            row = update_row_info(row_info=row, key='kast.toestand', value=kast.toestand.value)
            row = update_row_info(row_info=row, key='kast.assettype', value=kast.type.uri)
            row = update_row_info(row_info=row, key='kast.geometrie', value=kast_wkt_geometrie)

            logging.info("Update location of WV based on location of Kast")
            if kast_wkt_geometrie:
                eminfra_client.locatie_service.update_locatie_by_uuid(bron_asset_uuid=asset_uuid, wkt_geometry=kast_wkt_geometrie)
                row = update_row_info(row_info=row, key='wv.geometrie', value=kast_wkt_geometrie)
                row = update_row_info(row_info=row, key='opmerking', value='locatie overgenomen van Kast')

        rows.append(row)

    output_excel_path = ROOT_FOLDER / 'output_WV.xlsx'
    if output_excel_path.exists():
        # Append to existing file
        with pd.ExcelWriter(output_excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 1))
    else:
        # Write to a new file
        with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=(1, 1))