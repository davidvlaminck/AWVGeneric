import logging
import json

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd

from UseCases.utils import load_settings, build_query_search_betrokkenerelaties


def map_agent(name):
    with open("agent_mappings.json", "r", encoding="utf-8") as f:
        mappings = json.load(f)
    return mappings[name].get("new_value")


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Update (vervang) heeftbetrokkenerelatie van bepaalde agents met de waarde van een mapping dictionary')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    filepath = '[RSA] Asset (OTL) heeft een inactieve toezichter.xlsx'
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "naam_agent", "rol"])

    for idx, asset_row in df_assets.iterrows():
        asset = eminfra_client.get_asset_by_id(asset_id=asset_row.get("uuid"))

        agent_name_current = asset_row.get("naam_agent")
        agent_current = next(eminfra_client.search_agent(naam=agent_name_current, actief=False), None)
        agent_name_new = map_agent(name=agent_name_current)
        agent_new = next(eminfra_client.search_agent(naam=agent_name_new, actief=True), None)
        
        logging.debug(f'Processing asset: {asset.uuid}:')
        logging.debug(f"Update agent: '{agent_current.naam}' to '{agent_new.naam}'.")

        logging.info('Zoek een specifieke betrokkenerelatie, deactiveer deze en voeg een nieuwe toe met dezelfde rol.')
        query_betrokkenerelaties = build_query_search_betrokkenerelaties(bronAsset=asset)
        betrokkenerelaties = list(eminfra_client.search_betrokkenerelaties(query_dto=query_betrokkenerelaties))

        for betrokkenerelatie in betrokkenerelaties:
            if betrokkenerelatie.doel.get("naam") == agent_current.naam:
                try:
                    logging.info('Deactiveer betrokkenerelaties')
                    eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid=betrokkenerelatie.uuid)
                except Exception as e:
                    logging.critical(f'Error occured: "{e}"')

                try:
                    rol = betrokkenerelatie.rol
                    logging.info(f'Add new betrokkenerelatie with role: "{rol}"')
                    eminfra_client.add_betrokkenerelatie(asset=asset, agent_uuid=agent_new.uuid, rol=betrokkenerelatie.rol)
                except Exception as e:
                    logging.critical(f'Error occured: "{e}"')
