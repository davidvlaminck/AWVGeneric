import logging
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings, read_rsa_report


if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s', filemode="w")
    logging.info('EAN-nummers verplaatsen:\t EAN-nummers overdragen van assets (Legacy) naar DNBLaagspanning (OTL)')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=load_settings())

    filepath = Path().home() / 'Nordend/AWV - Documents/ReportingServiceAssets/Report0144/input' / '[RSA] Assets (legacy) met ingevuld kenmerk_ _elektrische aansluiting_.xlsx'
    usecols = ['uuid', 'naam', 'naampad', 'isTunnel', 'toestand', 'assettype_naam', 'ean', 'aansluiting']
    df_assets = read_rsa_report(filepath, usecols=usecols)

    # kenmerk elektrisch aansluitpunt is een apart kenmerk in em-infra
    # kenmerk ophalen
    # GET
    # '/api/assets/{assetId}/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b'

    # ofwel de algemene search-opdracht
    # POST
    # 'https://apps.mow.vlaanderen.be/eminfra/core/api/assets/026db587-05d9-4fc2-a5e2-9a55f12862f8/kenmerken/search'
    # {"size": 100, "from": 0, "selection": {"expressions": [{"terms": [
    #     {"property": "type.id", "value": "87dff279-4162-4031-ba30-fb7ffd9c014b", "operator": 0, "logicalOp": null,
    #      "negate": false}], "logicalOp": null}], "settings": {}},
    #  "expansions": {"fields": ["kenmerk:87dff279-4162-4031-ba30-fb7ffd9c014b"]}, "pagingMode": "OFFSET"}
    asset_uuid_dummy = '03d3fb2a-aaef-4988-b752-b0298c194063'
    elektrisch_aansluitpunt = eminfra_client.get_kenmerk_elektrisch_aansluitpunt(asset_uuid=asset_uuid_dummy)

    # kenmerk wijzigen (json-body leeg maken)
    # PUT
    # '/api/assets/{assetId}/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b'

    # Aansluiting loskoppelen
    # PUT
    # 'https://apps.mow.vlaanderen.be/eminfra/core/api/assets/026db587-05d9-4fc2-a5e2-9a55f12862f8/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b'
    # payload = {}
    # response is een 202

    logging.info('Filter assets met aansluiting = "A11.FICTIEF". Wis deze eigenschap')
    df_assets_wissen = df_assets[df_assets['aansluiting'] == 'A11.FICTIEF']

    # # update de eigenschap met een lege value.
    # eminfra_client.get_eigenschapwaarden(assetId=, eigenschap_naam='ean-nummer')
    # eminfra_client.update_eigenschap()
    #
    # for _, asset in df_assets.iterrows():
    #     print("Implement function logic here")