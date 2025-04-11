from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import BoomstructuurAssetTypeEnum
from API.Enums import AuthType, Environment
from pathlib import Path

print(""""
        Functionaliteiten gelinkt aan Beheerobject
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    return settings_path


if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    ##########################################
    # Oplijsten van beheerobject types
    ##########################################
    beheerobjecttypes = eminfra_client.get_beheerobjecttypes()
    beheerobjecttype_installatie = [item for item in beheerobjecttypes if item.naam == 'INSTAL (Beheerobject)'][0]

    ##########################################
    # Aanmaken van een beheerobject
    ##########################################
    response = eminfra_client.create_beheerobject(naam='dummyTest', beheerobjecttype=beheerobjecttype_installatie)
    print(f'Beheerobject aangemaakt met als uuid: {response.get("uuid")}')

    ##########################################
    # Ophalen van een specifiek beheerobject. Op basis van een uuid
    ##########################################
    myDummyBeheerobject_uuid = 'ded9c5bb-cafc-40a0-8816-9e679dde98df'
    myDummyBeheerobject = eminfra_client.get_beheerobject_by_uuid(beheerobject_uuid=myDummyBeheerobject_uuid)

    ##########################################
    # Oplijsten assets in de boomstructuur van een beheerobject
    ##########################################
    # WW0402 uuid: 0014b499-2bdb-448a-b3a1-fd20595701dc
    generator_beheerobjecten = eminfra_client.search_child_assets(asset_uuid='0014b499-2bdb-448a-b3a1-fd20595701dc', recursive=True)

    ##########################################
    # Zoek naar de parent-asset
    ##########################################
    # Diepgeneste LS asset uuid: 1fd580ca-9881-4325-8a8c-0d6c279ba607
    parent = eminfra_client.search_parent_asset(asset_uuid='1fd580ca-9881-4325-8a8c-0d6c279ba607')
    top_parent = eminfra_client.search_parent_asset(asset_uuid='1fd580ca-9881-4325-8a8c-0d6c279ba607', recursive=True)
    all_parents = eminfra_client.search_parent_asset(asset_uuid='1fd580ca-9881-4325-8a8c-0d6c279ba607', recursive=True, return_all_parents=True)

    ##########################################
    # Alle assets tesamen (de-)activeren
    ##########################################
    for item in generator_beheerobjecten:
        eminfra_client.deactiveer_asset(asset=item)
        eminfra_client.activeer_asset(asset=item)

    ##########################################
    # Wijzigen beheerobject
    ##########################################
    # to do bestaande asset toevoegen aan een installatie. Neemt 2 parameters als input: uuid_installatie, uuid_asset
    parentAsset = next(eminfra_client.search_asset_by_uuid(asset_uuid='201e5db4-7a3e-40e0-bba3-4947c5c4f725'), None)
    childAsset = next(eminfra_client.search_asset_by_uuid(asset_uuid='28cd12a8-03ee-4d6c-8c4a-40ec3ae1b10b'), None)
    installatie = eminfra_client.search_parent_asset(asset_uuid='28cd12a8-03ee-4d6c-8c4a-40ec3ae1b10b', recursive=True, return_all_parents=False)
    eminfra_client.reorganize_beheerobject(parentAsset=parentAsset, childAsset=childAsset, parentType=BoomstructuurAssetTypeEnum.ASSET)
    eminfra_client.reorganize_beheerobject(parentAsset=installatie, childAsset=childAsset, parentType=BoomstructuurAssetTypeEnum.BEHEEROBJECT)

    ##########################################
    # Wijzigen beheerobject
    ##########################################
    # to do: implementeren


    ##########################################
    # Zoeken van een beheerobject. Op basis van verschillende criteria met een QueryDTO.
    ##########################################
    # beheerobject_type = 'baa8570b-15cf-4512-a309-efd63af32f39'
    generator_beheerobjecten = eminfra_client.search_beheerobjecten(naam='JE')