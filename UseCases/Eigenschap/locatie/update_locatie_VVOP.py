import logging
import os.path
from pathlib import Path

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from API.eminfra.EMInfraDomain import OperatorEnum, QueryDTO, PagingModeEnum, SelectionDTO, TermDTO, \
    ExpressionDTO, LogicalOpEnum, LocatieKenmerk, AssetDTO

from UseCases.utils import load_settings_path, configure_logger

from typing import Iterable
from shapely import wkt
from shapely.geometry import GeometryCollection

ROOT_FOLDER = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0222'
INPUT_FILE = os.path.join(ROOT_FOLDER, 'Report0222.xlsx')


def initiate_row() -> dict:
    """
    Returns a row dictionary
    """
    return {
        "vvopgroep.uuid": None,
        "vvopgroep.naam": None,
        "vvopgroep.toestand": None,
        "vvopgroep.assettype": None,
        "vvopgroep.geometrie": None,
        "kast.uuid": None,
        "kast.naam": None,
        "kast.toestand": None,
        "kast.assettype": None,
        "kast.geometrie": None,
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


def get_wkts_from_assets(assets: list[AssetDTO]) -> list[str]:
    """
    Converts the assets to a lis of wkt's.
    """
    wkts = []
    for asset in assets:
        locatie_kenmerk = eminfra_client.locatie_service.get_locatie_by_uuid(asset_uuid=asset.uuid)
        wkt = parse_locatie_kenmerk_2_wkt(locatie_kenmerk=locatie_kenmerk)
        wkts.append(wkt)
    return wkts

def centerpoint_wkt_from_geometries(wkt_geometries: Iterable[str]) -> str | None:
    """
    Returns the WKT POINT string of the center of the bounding box
    around a collection of WKT geometries.

    :param wkt_geometries: Iterable of WKT strings
    :return: WKT string of center point
    """
    geometries = [wkt.loads(g) for g in wkt_geometries if g]

    if not geometries:
        return None
        # raise ValueError("No valid geometries provided")

    collection = GeometryCollection(geometries)
    minx, miny, maxx, maxy = collection.bounds

    center_x = (minx + maxx) / 2
    center_y = (miny + maxy) / 2

    return f"POINT({center_x} {center_y} 0)"




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
        logging.info(f"Processing VVOP-Groep: ({idx + 1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
        vvopgroep = eminfra_client.asset_service.get_asset_by_uuid(asset_uuid=asset_uuid)
        row = update_row_info(row_info=row, key='vvopgroep.uuid', value=vvopgroep.uuid)
        row = update_row_info(row_info=row, key='vvopgroep.naam', value=vvopgroep.naam)
        row = update_row_info(row_info=row, key='vvopgroep.toestand', value=vvopgroep.toestand.value)
        row = update_row_info(row_info=row, key='vvopgroep.assettype', value=vvopgroep.type.uri)

        logging.info("Search VVOP in VVOPGroep")
        child_assets = list(eminfra_client.asset_service.search_child_assets_by_uuid_generator(asset_uuid=vvopgroep.uuid, recursive=True))
        vvops = [a for a in child_assets if a.type.uri == 'https://lgc.data.wegenenverkeer.be/ns/installatie#VVOP']

        if vvops:
            logging.info("Get locations of VVOP")
            wkt_list = get_wkts_from_assets(assets=vvops)

            logging.info("Get centerpoint around bounding box of VVOP's")
            centerpoint_wkt_vvopgroep = centerpoint_wkt_from_geometries(wkt_list)

            if centerpoint_wkt_vvopgroep:
                eminfra_client.locatie_service.update_locatie_by_uuid(bron_asset_uuid=asset_uuid, wkt_geometry=centerpoint_wkt_vvopgroep)
                row = update_row_info(row_info=row, key='vvopgroep.geometrie', value=centerpoint_wkt_vvopgroep)
                row = update_row_info(row_info=row, key='opmerking', value='Locatie overgenomen van de VVOP''s.')
                rows.append(row)
                continue

        logging.info("Update location of VVOPGroep based on location of Kast")
        logging.info("Search installatie")
        installatie = eminfra_client.asset_service.search_parent_asset_by_uuid(
            asset_uuid=asset_uuid, return_all_parents=False, recursive=True)
        if not installatie:
            logging.warning('VVOP Groep (Legacy) zit niet in een boomstructuur.')
            row = update_row_info(row_info=row, key='opmerking', value='VVOP Groep (Legacy) staat niet in een boomstructuur')
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
            if kast_wkt_geometrie:
                eminfra_client.locatie_service.update_locatie_by_uuid(bron_asset_uuid=asset_uuid, wkt_geometry=kast_wkt_geometrie)
                row = update_row_info(row_info=row, key='vvopgroep.geometrie', value=kast_wkt_geometrie)
                row = update_row_info(row_info=row, key='opmerking', value='locatie overgenomen van Kast')

                row = update_row_info(row_info=row, key='kast.uuid', value=kast.uuid)
                row = update_row_info(row_info=row, key='kast.naam', value=kast.naam)
                row = update_row_info(row_info=row, key='kast.toestand', value=kast.toestand.value)
                row = update_row_info(row_info=row, key='kast.assettype', value=kast.type.uri)
                row = update_row_info(row_info=row, key='kast.geometrie', value=kast_wkt_geometrie)

        rows.append(row)

    output_excel_path = ROOT_FOLDER / 'output_VVOP.xlsx'
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