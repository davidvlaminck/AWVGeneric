import logging

from API.eminfra.EMInfraClient import EMInfraClient
from API.eminfra.EMInfraDomain import (AssetDTO, QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO,
                                       OperatorEnum, LogicalOpEnum, AgentDTO)
from API.Enums import AuthType, Environment
import pandas as pd
from UseCases.utils import load_settings

print("""
        Update van de toezichters van OTL-assets, Kris Smet en Ruben Henrard, naar Lantis
      """)


def build_query_search_assets(toezichter: AgentDTO):
    return QueryDTO(
        size=100,
        from_=0,
        pagingMode=PagingModeEnum.OFFSET,
        selection=SelectionDTO(
            expressions=[
                ExpressionDTO(
                terms=[
                    TermDTO(property='actief', operator=OperatorEnum.EQ, value=True)
                ]),
                ExpressionDTO(
                    terms=[
                        TermDTO(property='agent', operator=OperatorEnum.EQ, value=f'{toezichter.uuid}:toezichter')
                    ], logicalOp=LogicalOpEnum.AND)
            ]
        )
    )

def build_query_search_betrokkenerelaties(asset: AssetDTO):
    return QueryDTO(size=5, from_=0, pagingMode=PagingModeEnum.OFFSET,
                                 selection=SelectionDTO(expressions=[
                                     ExpressionDTO(terms=[
                                         TermDTO(property='bronAsset', operator=OperatorEnum.EQ,
                                                 value=f'{asset.uuid}')])]))

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('Update toezichter naar LANTIS:\t Toezichters Kristof Smet en Ruben Henrard')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    toezichter_lantis = next(eminfra_client.search_agent(naam='LANTIS', actief=True), None)
    toezichter_namen = ['Ruben Henrard', 'Kristof Smet']

    for toezichter_naam in toezichter_namen:
        # Get ID's of toezichters Ruben Henrard en Kristof Smet
        toezichter = next(eminfra_client.search_agent(naam=toezichter_naam, actief=True), None)

        if not toezichter:
            logging.critical(f'Toezichter: {toezichter_naam} returned no assets.')
            continue

        # Launch query to get all OTL-assets with toezichter Ruben Henrard en Kristof Smet
        query_assets = build_query_search_assets(toezichter=toezichter)
        assets = eminfra_client.search_assets(query_dto=query_assets)

        logging.info(f'Processing all OTL-assets for which the toezichter is: {toezichter.naam}')
        data = []
        for asset in iter(assets):
            logging.info(f'Processing asset: {asset.uuid}')
            data.append(
                {"eminfra": f'https://apps.mow.vlaanderen.be/eminfra/assets/{asset.uuid}', "uuid": asset.uuid,
                 "naam": asset.naam, "typeURI": asset.type.korteUri, "actief": asset.actief, "toestand": asset.toestand.value}
            )

            # # Remove the existing betrokkenerelatie (Ruben/Kristof)
            logging.info('Remove existing betrokkenerelatie')
            query_betrokkenerelaties = build_query_search_betrokkenerelaties(asset=asset)
            betrokkenerelaties = list(eminfra_client.search_betrokkenerelaties(query_dto=query_betrokkenerelaties))

            for betrokkenerelatie in betrokkenerelaties:
                if betrokkenerelatie.rol == 'toezichter' and betrokkenerelatie.doel.get("uuid") == toezichter.uuid:
                    logging.info(f'Deactiveer betrokkenerelaties van de toezichter {toezichter.naam}')
                    eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid=betrokkenerelatie.uuid)

            if not [item for item in betrokkenerelaties if item.rol == 'toezichter' and item.doel.get("uuid") == toezichter_lantis.uuid]:
                # Add a new betrokkenerelatie Lantis
                logging.info('Add new betrokkenerelatie LANTIS')
                eminfra_client.add_betrokkenerelatie(asset=asset, agent_uuid=toezichter_lantis.uuid, rol='toezichter')

        # Convert to DataFrame
        df_assets = pd.DataFrame(data)

        # Write to Excel
        df_assets.to_excel(f"OTL_assets_{toezichter.naam}_inspection.xlsx", index=False, freeze_panes=[1,2])