import logging
from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
import pandas as pd
from pathlib import Path
from UseCases.utils import load_settings, configure_logger, read_rsa_report

if __name__ == '__main__':
    configure_logger()
    logging.info(
        'Toevoegen van een toezichtsgroep op basis van een mapping bestand.\n'
        'Het mapping bestand bevat de naam van agenten en diens bijbehorende toezichtsgroep.'
    )
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info('Read input Excel file into df.')
    excel_path = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0191' / '[RSA] Laagspanningsgedeelte (Legacy) keuringsinfo.xlsx'
    usecols = ['uuid', 'naampad', 'uri', 'toezichter_naam_voornaam', 'toezichter_gebruikersnaam', 'toezichtgroep_naam']
    df_assets = read_rsa_report(filepath=excel_path, usecols=usecols)

    logging.info('Read the complete mapping file (json) in a pandas dataframe')
    df_mapping = pd.read_json('mapping_agent_toezichtgroepen.json')
    df_mapping.rename(columns={"toezichter": "toezichter_gebruikersnaam", "toezichtsgroep": "toezichtsgroep_nieuw"}, inplace=True)

    logging.info('Join toezichtsgroep op basis van de naam van de agent. Kolomnaam: "toezichtsgroep_nieuw"')
    df_assets = df_assets.merge(df_mapping, how='left', on="toezichter_gebruikersnaam", validate='many_to_one')

    logging.info('Filter dataframe waarbij "toezichtsgroep_nieuw" is niet NaN.')
    df_assets = df_assets.dropna(subset=['toezichtsgroep_nieuw'])

    logging.info('Filter dataframe waarbij "toezichtsgroep" != "toezichtsgroep_nieuw"')
    df_assets = df_assets[df_assets["toezichtgroep_naam"] != df_assets["toezichtsgroep_nieuw"]]

    df_assets.reset_index(drop=True, inplace=True)

    for idx, df_row in df_assets.iterrows():
        asset = eminfra_client.get_asset_by_id(df_row['uuid'])
        logging.debug(f'Processing asset: ({idx+1}/{len(df_assets)}): {asset.uuid}')

        try:
            toezichter = next(eminfra_client.search_identiteit(naam=df_row["toezichter_gebruikersnaam"]), None)
            toezichtsgroep = next(eminfra_client.search_toezichtgroep_lgc(naam=df_row["toezichtsgroep_nieuw"]), None)
        except Exception as e:
            raise Exception(f'Problem occured: "{e}"') from e
        logging.info(f'Nieuwe toezichter: {toezichter.naam}')
        logging.info(f'Nieuwe toezichtsgroep: {toezichtsgroep.naam}')
        eminfra_client.add_kenmerk_toezichter_by_asset_uuid(asset_uuid=asset.uuid, toezichter_uuid=toezichter.uuid, toezichtgroep_uuid=toezichtsgroep.uuid)