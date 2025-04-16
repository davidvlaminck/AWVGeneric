from geopandas import GeoDataFrame
from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import geopandas as gpd
from pathlib import Path

print(""""
        Update geometrie van de Asset (Legacy) via de em-infra API
        Inlezen van GeoJSON en update van Beheerobjecten van de nieuwe wegsegmenten (A-wegen, N-wegen, Rwegen, ...) 
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path

def read_json(filepath: Path, usecols=list[str] | None) -> gpd.GeoDataFrame:
    """ Reads GeoJSON in Pandas dataframe

    :param filepath:
    :param usecols:
    :return:
    """
    if usecols is None:
        usecols = ["id", "naampad", "geometry"]
    gdf = gpd.read_file(filepath)
    return gdf[usecols]


if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    json_path = Path().home()
    df_assets: GeoDataFrame = read_json(filepath=json_path, usecols=["id", "naampad", "geometry"])

    for idx, asset in df_assets.iterrows():
        print(f'asset: {asset.id}')

        # get wkt_geometry
        locatieKenmerk = eminfra_client.get_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.id)
        wkt_geometrie = locatieKenmerk.geometrie

        if asset.geometry.wkt == wkt_geometrie:
            print("\tGeometrie is ongewijzigd/identiek, geen verdere actie")
            continue

        else:
            print("\tUpdate geometry")
            eminfra_client.update_kenmerk_locatie_by_asset_uuid(asset_uuid=asset.id, wkt_geom=asset.geometry)