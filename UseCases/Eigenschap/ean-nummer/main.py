import logging
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('EAN-nummers verplaatsen:\t EAN-nummers overdragen van assets (Legacy) naar DNBLaagspanning (OTL)')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    filepath = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0144/input' / '[RSA] Assets (legacy) met ingevuld kenmerk_ _elektrische aansluiting_.xlsx'
    usecols = ['uuid', 'naam', 'naampad', 'isTunnel', 'toestand', 'assettype_naam', 'ean', 'aansluiting']
    df_assets = read_rsa_report(filepath, usecols=usecols)

    # kenmerk ophalen
    asset_uuid_dummy = '03d3fb2a-aaef-4988-b752-b0298c194063'
    elektrisch_aansluitpunt = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)

    # asset_uuid_dummy = '04a23cea-e851-4d81-b4df-2a9a18b17414'
    # elektrisch_aansluitpunt = eminfra_client.search_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)

    # kenmerk wijzigen (json-body leeg maken)
    # PUT
    # '/api/assets/{assetId}/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b'

    # Aansluiting loskoppelen
    eminfra_client.disconnect_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)


    logging.info('Filter assets met aansluiting = "A11.FICTIEF". Wis deze eigenschap')
    df_assets_wissen = df_assets[df_assets['aansluiting'] == 'A11.FICTIEF']

    # # update de eigenschap met een lege value.
    # eminfra_client.get_eigenschapwaarden(assetId=, eigenschap_naam='ean-nummer')
    # eminfra_client.update_eigenschap()
    #
    # for _, asset in df_assets.iterrows():
    #     print("Implement function logic here")