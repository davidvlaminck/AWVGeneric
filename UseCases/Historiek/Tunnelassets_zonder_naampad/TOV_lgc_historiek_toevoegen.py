import logging

import pandas as pd

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, configure_logger

if __name__ == '__main__':
    configure_logger()
    logging.info('TOV Legacy assets zonder naampad.\nHistoriek toevoegen: aanmaakdatum en gebruiker.')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    input_file = "TOV_lgc_zonder_naampad.xlsx"
    usecols = ['id', 'type', 'actief', 'toestand']
    df_assets = pd.read_excel(io=input_file, sheet_name='Sheet1', usecols=usecols, header=0)

    rows = []
    for idx, df_row in df_assets.iterrows():
        asset_uuid = df_row["id"]
        logging.info(f"Processing asset: ({idx+1}/{len(df_assets)}): asset_uuid: {asset_uuid}")
        asset = eminfra_client.get_asset_by_id(asset_id=asset_uuid)

        events = eminfra_client.search_events(asset_uuid=asset.uuid)

        for event in events:
            if event.type.name == 'INSTALLATIE_CREATED':
                aanmaakdatum = event.createdOn
                gebruiker_uuid = event.data.get("createdBy")
                identiteit = eminfra_client.get_identiteit(toezichter_uuid=gebruiker_uuid)
                gebruiker = f'{identiteit.voornaam} {identiteit.naam}'
        row = {
            "id": asset_uuid
            , "type": df_row["type"]
            , "actief": df_row["actief"]
            , "toestand": df_row["toestand"]
            , "aanmaakdatum": aanmaakdatum
            , "gebruiker": gebruiker
        }
        rows.append(row)

    output_excel_path = 'TOV_lgc_zonder_naampad_plus_historiek.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])