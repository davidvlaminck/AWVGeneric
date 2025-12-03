import geopandas as gpd

if __name__ == "__main__":
    # Load a specific layer from a GeoPackage file
    gdf = gpd.read_file("/home/davidlinux/Downloads/AwvData.gpkg", layer="fietspaden_wrapp")
    print(gdf.head())

    # print out the first row's geometry
    print(gdf.iloc[0].geometry)
    # print out the type of the geometry
    print(type(gdf.iloc[0].geometry))


    # https://apps.mow.vlaanderen.be/locatieservices2/technische-documentatie/swagger.html#/default/post_rest_lijnlocatie_batch_via_geometry_multi
    # https: // github.com / joachimdero / AwvFuncties_os / blob / master / libs / Locatieservices2.py
    # rest_puntlocatie_batch
    # wegnummer meegeven in body

    # tussen resultaat wegschrijven in aparte bestanden? of andere approach met finally?