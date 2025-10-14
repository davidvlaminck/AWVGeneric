import logging

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Ophalen eigenschappen van de beheeracties van Inspectie Wegverlichting, horend bij VPLMast.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    df_assets = pd.read_excel('Export masten C1383.xlsx', sheet_name='Sheet0', header=0, usecols=['id'])

    _, relatietype_beheeractie = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri='https://bz.data.wegenenverkeer.be/ns/onderdeel#HeeftBeheeractie')
    _, relatietype_bezoekt = eminfra_client.get_kenmerktype_and_relatietype_id(
        relatie_uri='https://bz.data.wegenenverkeer.be/ns/onderdeel#Bezoekt')
    relatieTypes = [relatietype_bezoekt, relatietype_beheeractie]

    rows = []
    for _, asset_row in df_assets.iterrows():
        logging.info("ophalen asset VPLMast")
        asset = eminfra_client.get_asset_by_id(assettype_id=asset_row.get("id"))
        row = {"asset_uuid": asset.uuid, "asset_naam": asset.naam}

        logging.info("Ophalen Graph, met een diepte van 2 stappen.")
        logging.info("Via de relatie Bezoekt -> InspectieWegverlichting")
        graph = eminfra_client.get_graph(asset_uuid=asset.uuid, depth=1, relatieTypes=[relatietype_bezoekt], actiefFilter=True)
        # todo parse het resultaat van de graph dictionary

        logging.info("via de relatie HeeftBeheerActie -> InspectieMastControle en InspectieToestel")
        # graph = eminfra_client.get_graph(asset_uuid=, depth=1, relatieTypes=[relatietype_beheeractie], actiefFilter=True)
        # todo parse het resultaat van de graph dictionary

        logging.info("Ophalen van de eigenschappen van beide beheeracties")

    logging.info("Aanvullen dataframe en wegschrijven naar Excel")