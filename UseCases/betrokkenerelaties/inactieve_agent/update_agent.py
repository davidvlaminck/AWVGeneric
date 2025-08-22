import logging
import json

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path


def load_settings():
    """Load API settings from JSON"""
    return Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'

def build_query_search_betrokkenerelaties(asset: AssetDTO):
    return QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 selection=SelectionDTO(expressions=[
                                     ExpressionDTO(terms=[
                                         TermDTO(property='bronAsset', operator=OperatorEnum.EQ,
                                                 value=f'{asset.uuid}')])]))

def map_agent(name):
    with open("agent_mappings.json", "r", encoding="utf-8") as f:
        mappings = json.load(f)
    return mappings[name].get("new_value")


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Update (vervang) heeftbetrokkenerelatie van bepaalde agents met de waarde van een mapping dictionary')
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    filepath = '[RSA] Asset (OTL) heeft een inactieve toezichter.xlsx'
    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, usecols=["uuid", "naam_agent", "rol"])

    for idx, asset_row in df_assets.iterrows():
        asset = eminfra_client.get_asset_by_id(assettype_id=asset_row.get("uuid"))

        agent_name_current = asset_row.get("naam_agent")
        agent_current = next(eminfra_client.search_agent(naam=agent_name_current, actief=False), None)
        agent_name_new = map_agent(name=agent_name_current)
        agent_new = next(eminfra_client.search_agent(naam=agent_name_new, actief=True), None)
        
        logging.debug(f'Processing asset: {asset.uuid}:')
        logging.debug(f"Update agent: '{agent_current.naam}' to '{agent_new.naam}'.")

        logging.info('Zoek een specifieke betrokkenerelatie, deactiveer deze en voeg een nieuwe toe met dezelfde rol.')
        query_betrokkenerelaties = build_query_search_betrokkenerelaties(asset=asset)
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
