from geopandas import GeoDataFrame

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, OperatorEnum, TermDTO, ExpressionDTO, SelectionDTO, QueryDTO, PagingModeEnum, \
    LogicalOpEnum
from API.Enums import AuthType, Environment
import pandas as pd
import geopandas as gpd
from pathlib import Path

print(""""
        Update geometrie van de Asset (Legacy) via de em-infra API
        Inlezen van GeoJSON en update van Beheerobjecten van de nieuwe wegsegmenten (A-wegen, N-wegen, ...) 
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def read_json(filepath: Path, usecols=list[str] | None) -> gpd.GeoDataFrame:
    if usecols is None:
        usecols = ["id", "naampad", "geometry"]
    # Read the GeoJSON file
    gdf = gpd.read_file(filepath)
    # clean up geodataframe
    # remove escape character '\n'
    gdf['id'] = gdf['id'].str.replace('\n', '', regex=False)
    return gdf[usecols]

def functie() -> None:
    """"""
    return None

if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    json_path = Path().home() / 'Downloads' / 'Beheersegment' / 'beheersegmentenGeometrieVTC_20250415.geojson'
    df_assets: GeoDataFrame = read_json(filepath=json_path, usecols=["id", "naampad", "geometry"])

    counter_errors = 0
    for idx, asset in df_assets.iterrows():
        print(f'asset: {asset.id}')

        if asset.id == 'nieuwe ID':
            print(f'Opzoeking nieuwe ID voor "naampad": {asset.naampad}')
            query_dto = QueryDTO(
                size=10, from_=0, pagingMode=PagingModeEnum.OFFSET,
                selection=SelectionDTO(
                expressions=[
                    # ExpressionDTO(terms=[TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)]),
                    ExpressionDTO(terms=[TermDTO(property='naamPad', operator=OperatorEnum.STARTS_WITH, value=asset.naampad)])
                ]))
            results = list(eminfra_client.search_assets(query_dto=query_dto))
            if len(results) != 1:
                # raise ValueError('Found multiple wegsegmenten')
                counter_errors += 1
                # todo
                # log error
                # skip this asset with continue statement

            asset.id = results[0].uuid # update asset met diens nieuwe uuid

        # get wkt_geometry
        locatieKenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.id)
        wkt_geometrie = locatieKenmerk.geometrie

        if asset.geometry.wkt == wkt_geometrie:
            print("\tGeometrie is ongewijzigd/identiek, geen verdere actie")
        else:
            # print(f"\tHuidige geometrie: {asset_geometrie.wkt}\n\tNieuwe geometrie: {wkt_geometrie}")
            print("\tUpdate geometry")

        # wkt_geometrie = eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=, wkt_geom=)
