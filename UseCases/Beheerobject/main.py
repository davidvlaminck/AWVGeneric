from API.EMInfraClient import EMInfraClient
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
    # eminfra_client.create_beheerobject(naam='dummyTest', beheerobjecttype=beheerobjecttype_installatie)
    response = eminfra_client.create_beheerobject(naam='dummyTest4')
    print(f'Beheerobject aangemaakt met als uuid: {response.get("uuid")}')

    ##########################################
    # Ophalen van een specifiek beheerobject. Op basis van een uuid
    ##########################################
    myDummyBeheerobject_uuid = 'ded9c5bb-cafc-40a0-8816-9e679dde98df'
    myDummyBeheerobject = eminfra_client.get_beheerobject_by_uuid(beheerobject_uuid=myDummyBeheerobject_uuid)

    ##########################################
    # Wijzigen beheerobject
    ##########################################
    # to do: implementeren

    ##########################################
    # Zoeken van een beheerobject. Op basis van verschillende criteria met een QueryDTO.
    ##########################################
    beheerobject_type = 'baa8570b-15cf-4512-a309-efd63af32f39'
    generator_beheerobjecten = eminfra_client.search_beheerobjecten(naam='JE', beheerobjecttype=beheerobjecttype_installatie, actief=True)
    lst_generator_beheerobjecten = list(generator_beheerobjecten)