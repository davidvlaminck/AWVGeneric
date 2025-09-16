import json
from itertools import chain

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import TermDTO, QueryDTO, ExpressionDTO, SelectionDTO, OperatorEnum, PagingModeEnum, \
    LogicalOpEnum, ApplicationEnum
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path

from Generic.ExcelModifier import ExcelModifier
from UseCases.utils import load_settings

print("""
    Ophalen van informatie via verschillende eminfra API calls en bewaren van de info in een Pandas dataframe.
    Wegschrijven van het dataframe naar Excel, ter controle van de aanpassingen.
    Nadien de toezichter en toezichgroep updaten via een API-call.
    
    In deze use-case gaan we op zoek naar de historiek van assets gelinkt aan een specifieke toezichter en herstellen we de vorige toezichter.
    
    df_assets = pd.DataFrame(columns=
        ["asset_uuid"
        , "assettype"
        , "toezichter_actueel_naam", "toezichter_actueel_uuid"
        , "toezichter_vorig_naam", "toezichter_vorig_uuid"
        , "toezichter_nieuw_naam", "toezichter_nieuw_uuid"])
    
""")

if __name__ == '__main__':
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    # intialize an empty dataframe
    df_assets = pd.DataFrame(columns=
        ["asset_uuid", "assettype"
        , "toezichter_actueel_uuid", "toezichter_actueel_naam"
        , "toezichtgroep_actueel_uuid", "toezichtgroep_actueel_naam"
        , "toezichter_vorig_uuid", "toezichter_vorig_naam"
        , "toezichtgroep_vorig_uuid", "toezichtgroep_vorig_naam"
        , "toezichter_nieuw_uuid", "toezichter_nieuw_naam"
        , "toezichtgroep_nieuw_uuid", "toezichtgroep_nieuw_naam"])

    # initialize the event type
    eventtypes = list(eminfra_client.get_all_eventtypes())
    event_aanpassing_toezichter = next(event for event in eventtypes if event.description == 'Toezichter van asset aangepast')
    event_aanpassing_toezichtgroep = next(event for event in eventtypes if event.description == 'Toezichtgroep van asset aangepast')

    # dataframe met toezichters
    with open('toezichters.json', 'r') as file:
        toezichters_data = json.load(file)
    df_toezichters = pd.DataFrame(data=toezichters_data)
    toezichter_of_interest_uuid = df_toezichters[df_toezichters["uuid"] == 'a3133f89-7a0f-41ff-bcd9-49e58ba2fd3a']["uuid"][0]

    # dataframe met toezichtgroepen
    with open('toezichtgroepen.json') as file:
        toezichtgroepen_data = json.load(file)
    df_toezichtgroepen = pd.DataFrame(data=toezichtgroepen_data)

    # Ophalen assets.
    # Bi-Flashinstallatie (Legacy); toestand = actief; toezichter (#50)
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[
                                     TermDTO(property='type', operator=OperatorEnum.EQ, value='fd45b1f3-13cb-49f7-a33b-b644e683d7dc')
                                     , TermDTO(property='actief', operator=OperatorEnum.EQ,
                                               value=True, logicalOp=LogicalOpEnum.AND)
                                     , TermDTO(property='toezichter', operator=OperatorEnum.EQ,
                                               value=toezichter_of_interest_uuid, logicalOp=LogicalOpEnum.AND)])]))
    assets_bif = eminfra_client.search_all_assets(query_dto=query_dto)

    # Bochtafbakeningsinstallaties (Legacy) actief; toezichter (#9)
    query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                         selection=SelectionDTO(
                             expressions=[ExpressionDTO(
                                 terms=[
                                     TermDTO(property='type', operator=OperatorEnum.EQ, value='2ec89769-3b9a-4cf4-9a31-11621257f470')
                                     , TermDTO(property='actief', operator=OperatorEnum.EQ,
                                                value=True, logicalOp=LogicalOpEnum.AND)
                                     , TermDTO(property='toezichter', operator=OperatorEnum.EQ,
                                                value=toezichter_of_interest_uuid, logicalOp=LogicalOpEnum.AND)])]))
    assets_baf = eminfra_client.search_assets(query_dto=query_dto)

    # whitelist of assets in the municipalities Deinze and Zulte
    uuid_whitelist_deinze = ['767bcacb-8e33-4037-96ff-3d477c785e71', '7c85114f-13fd-4f60-8544-75c82c388d59', '8b6e4897-13dd-41cd-bc9f-0c1a0c8330c0']
    uuid_whitelist_zulte = ['72c643af-21cd-4356-ab61-6919cd969e6f', '1ca2a0fb-f4bd-4c68-af9b-97e859362d53']
    uuid_whitelist = uuid_whitelist_deinze + uuid_whitelist_zulte

    # assets van Bi-Flash (Legacy) en Bochtafbakening (Legacy) samenvoegen
    assets = chain(assets_bif, assets_baf)
    # Toevoegen van de data aan het dataframe
    for index, asset in enumerate(assets):
        print(f'Processing dataframe for asset: {asset.uuid}')
        df_assets.loc[index, "asset_uuid"] = asset.uuid
        df_assets.loc[index, "assettype"] = asset.type.korteUri
        df_assets.loc[index, "toezichter_actueel_uuid"] = toezichter_of_interest_uuid

        # Ophalen van de vorige toezichthouder voor een specifieke asset
        generator_events = eminfra_client.search_events(asset_uuid=asset.uuid, event_type=event_aanpassing_toezichter)
        if event := next(generator_events, None):
            df_assets.loc[index, "toezichter_vorig_uuid"] = event.data.get("from", {}).get("uuid", None)
            df_assets.loc[index, "toezichter_actueel_uuid"] = event.data.get("to", {}).get("uuid", None)

        # Ophalen van de vorige toezichtgroep voor een specifieke asset
        generator_events = eminfra_client.search_events(asset_uuid=asset.uuid, event_type=event_aanpassing_toezichtgroep)
        if event := next(generator_events, None):
            df_assets.loc[index, "toezichtgroep_vorig_uuid"] = event.data.get("from", {}).get("uuid", None)
            df_assets.loc[index, "toezichtgroep_actueel_uuid"] = event.data.get("to", {}).get("uuid", None)

        # tot slot, de toekomstige toezichter "toezichter_nieuw_uuid" toekennen op basis van de beschikbare logica.
        # controleer de uitzondering: BIF in Deinze of Zulte >> specifieke toezichter
        if df_assets.loc[index, "asset_uuid"] in uuid_whitelist:
            df_assets.loc[index, "toezichter_nieuw_uuid"] = df_toezichters[df_toezichters["uuid"] == 'd1490e77-655a-46de-b4eb-c6d788309553']["uuid"].item()
        # de vorige toezichter was persoon X >> nieuwe toezichter: persoon Y
        elif df_assets.loc[index, "toezichter_vorig_uuid"] == '7f077ae3-ebaf-41ef-89e0-e89475fd9c45':
            df_assets.loc[index, "toezichter_nieuw_uuid"] = df_toezichters[df_toezichters["uuid"] == '9aec6952-368b-4a51-8ee0-02231ad339f8']["uuid"].item()
        # Herstel de vorige toezichter
        else:
            df_assets.loc[index, "toezichter_nieuw_uuid"] = df_assets.loc[index, "toezichter_vorig_uuid"]

    # alle toezichtsgroepen naar AWV_EW_OV
    df_assets["toezichtgroep_nieuw_uuid"] = df_toezichtgroepen[df_toezichtgroepen["naam"] == 'AWV_EW_OV']["uuid"].item()

    # Fill the "toezichter_naam" column using the "uuid" column as a key
    df_assets["toezichter_vorig_naam"] = df_assets["toezichter_vorig_uuid"].map(df_toezichters.set_index("uuid")["naam"])
    df_assets["toezichter_actueel_naam"] = df_assets["toezichter_actueel_uuid"].map(df_toezichters.set_index("uuid")["naam"])
    df_assets["toezichter_nieuw_naam"] = df_assets["toezichter_nieuw_uuid"].map(df_toezichters.set_index("uuid")["naam"])

    df_assets["toezichtgroep_vorig_naam"] = df_assets["toezichtgroep_vorig_uuid"].map(df_toezichtgroepen.set_index("uuid")["naam"])
    df_assets["toezichtgroep_actueel_naam"] = df_assets["toezichtgroep_actueel_uuid"].map(df_toezichtgroepen.set_index("uuid")["naam"])
    df_assets["toezichtgroep_nieuw_naam"] = df_assets["toezichtgroep_nieuw_uuid"].map(df_toezichtgroepen.set_index("uuid")["naam"])

    # Dataframe assets wegschrijven ter controle van de wijzigingen
    excel_path = Path().home() / 'Downloads' / 'toezichter' / 'terugzetten_toezichter.xlsx'
    df_assets.to_excel(excel_path, sheet_name='geschiedenis_toezichters', index=False, freeze_panes=(1,1))
    excelmodifier = ExcelModifier(excel_path)
    excelmodifier.add_hyperlink()

    #################################################################################
    ####  Wijzig kenmerk toezichter / toezichtgroep
    #################################################################################
    # Nieuwe toezichter en toezichtsgroep toekennen in eminfra
    print("Wijzigen toezichter en toezichthouder")
    for index, row in df_assets.iterrows():
        uuid_asset = row["asset_uuid"]
        uuid_toezichter = row["toezichter_nieuw_uuid"]
        uuid_toezichtgroep = row["toezichtgroep_nieuw_uuid"]
        print(f"update asset: {uuid_asset}")

        json_body = {
                "toezichter": {
                    "uuid": f"{uuid_toezichter}"
                },
                "toezichtGroep": {
                    "uuid": f"{uuid_toezichtgroep}"
                }
            }
        try:
            eminfra_client.update_kenmerk(asset_uuid=uuid_asset, kenmerk_uuid='f0166ba2-757c-4cf3-bf71-2e4fdff43fa3', request_body=json_body)
        except:
            print("update failed")