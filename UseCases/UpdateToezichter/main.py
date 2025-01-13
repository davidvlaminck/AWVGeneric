import requests

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import QueryDTO, PagingModeEnum, SelectionDTO, ExpressionDTO, TermDTO, OperatorEnum, \
    LogicalOpEnum, ExpansionsDTO
from API.Enums import AuthType, Environment

import pandas as pd
import openpyxl
import polars as pd # check dit

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('C:/Users/DriesVerdoodtNordend/OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    #################################################################################
    ####  Read RSA-report as input
    #################################################################################
    filepath = Path(r"C:\Users\DriesVerdoodtNordend\Downloads\RSA Bijhorende assets hebben een verschillende toezichtshouder_toezichtsgroep (assettype = Signaalkabel).xlsx")

    df_assets = pd.read_excel(filepath, sheet_name='Resultaat', header=2, nrows=5)
    otl_uuids = df_assets.loc[:, 'otl_uuid']
    lgc_uuids = df_assets.loc[:, 'lgc_uuid']

    #################################################################################
    ####  Get betrokkenerelatie toezichter from OTL-asset
    #################################################################################
    # Get all betrokkenerelaties.
    # Check if there is only 1 toezichter. This is the default behaviour.
    sample_asset = df_assets.iloc[0, :]
    asset_uuid_otl = sample_asset['otl_uuid']
    asset_uuid_lgc = sample_asset['lgc_uuid']
    type_term = TermDTO(property='bronAsset', operator=OperatorEnum.EQ, value=asset_uuid_otl)
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['parent']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[type_term
                                        # TODO extra filter toevoegen: rol=toezichter
                                        # , TermDTO(property='actief', operator=OperatorEnum.EQ,
                                        #         value=True, logicalOp=LogicalOpEnum.AND)
                                        ])]))

    # betrokkenerelaties = eminfra_client.search_betrokkenerelaties(query_dto=query_dto)
    betrokkenerelaties = []
    # TODO gebruik de andere endpoint otl/betrokkenerelatie/search
    betrokkenerelaties.extend(betrokkenerelatie.doel for betrokkenerelatie in eminfra_client.search_betrokkenerelaties(query_dto))

    if len(betrokkenerelaties) == 1:
        betrokkenerelatie_uuid_otl = betrokkenerelaties[0].get('uuid')
    else:
        #TODO error catching
        raise ValueError(f'Too much betrokkenen for asset: {asset_uuid_otl}')

    #################################################################################
    ####  Get betrokkenerelatie toezichter from the LGC-asset
    #################################################################################
    lgc_toezichthouder = f'{sample_asset["lgc_toezichthouder_voornaam"]} {sample_asset["lgc_toezichthouder_naam"]}'

    type_term = TermDTO(property='naam', operator=OperatorEnum.EQ, value=lgc_toezichthouder)
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         expansions=ExpansionsDTO(fields=['contactInfo']),
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[type_term
                                        , TermDTO(property='actief', operator=OperatorEnum.EQ,
                                                value=True, logicalOp=LogicalOpEnum.AND)
                                        ])]))

    agents = []
    # TODO indien mogelijk, gebruik OTL-endpoint
    agents.extend(iter(eminfra_client.search_agent(query_dto=query_dto)))

    if len(agents) == 1:
        betrokkenerelatie_uuid_lgc = agents[0].uuid
    else:
        # TODO error catching
        raise ValueError(f'Multiple Agents found for the name: {lgc_toezichthouder}')


    #################################################################################
    ####  Add new betrokkenerelatie toezichter to OTL-asset
    #################################################################################
    json_data = {
        "bron": {
            "uuid": f"{asset_uuid_otl}"
            , "_type": "onderdeel"
        },
        "doel": {
            "uuid": f"{betrokkenerelatie_uuid_lgc}"
            , "_type": "agent"
        },
        "rol": "toezichter"
    }

    # Send the POST request
    try:
        response = eminfra_client.requester.post(url='core/api/betrokkenerelaties', json=json_data)

        # Check if the request was successful
        if response.status_code in (200, 202): # todo idem response code kiezen
            print("Request successful!")
            print("Response:", response.json())  # Parse JSON response
        else:
            print(f"Request failed with status code {response.status_code}")
            print("Response content:", response.text)

    except requests.exceptions.RequestException as e:
        print("An error occurred:", e)

    #################################################################################
    ####  Remove betrokkenerelatie toezichter from OTL-asset
    #################################################################################
    # TODO response nakijken. De response wordt niet gebruikt, dus die kan gewist worden.
    # TODO Test wissen pas nadat eerst een nieuwe betrokkenerelatie "toezichter" werd toegevoegd.
    # TODO hier niet de agent-id, maar effectief de betrokkenerelatie-id meegeven. Eerst deze id nog opvissen?
    response = eminfra_client.remove_betrokkenerelatie(betrokkenerelatie_uuid_otl)
    print('end of file')