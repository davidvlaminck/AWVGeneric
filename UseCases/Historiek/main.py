from datetime import datetime

from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    #################################################################################
    ####  Search events
    #################################################################################
    # Search all events on a specific date
    today = datetime.now()
    events = eminfra_client.search_events(created_after=today, created_before=today)
    print(list(events))

    # Search all events created by a specific user in the last month
    identiteiten = eminfra_client.search_identiteit(naam='Dries Verdoodt')
    identiteit_me = next(filter(lambda ident: ident.gebruikersnaam == 'saaaa15307', identiteiten),
                      None)
    datetime_first_of_month = datetime.now().replace(day=1)
    events = eminfra_client.search_events(created_after=datetime_first_of_month, created_by=identiteit_me)
    print(list(events))

    # Search all events linked to a specific asset
    dummy_asset_uuid = '1f622605-aa54-47eb-ab06-2d77e12f1d2a' # fiets tel lus Tervuren
    events = eminfra_client.search_events(asset_uuid=dummy_asset_uuid)
    print(list(events))

    # Search all events linked to a specific context. For example aanlevering
    event_contexten = eminfra_client.search_eventcontexts(omschrijving='DA-2025-00001')
    event_context = next(iter(event_contexten))
    events = eminfra_client.search_events(event_context=event_context)
    print(list(events))

    # Search all events of a specific person and event_type, after a certain date
    datetime_first_of_month = datetime.now().replace(day=1)
    identiteiten = eminfra_client.search_identiteit(naam='Dries Verdoodt')
    identiteit_me = next(filter(lambda ident: ident.gebruikersnaam == 'saaaa15307', identiteiten),
                      None)
    event_types = eminfra_client.get_all_eventtypes()
    event_type = next(filter(lambda item: item.description == 'Eigenschappen van asset kenmerk aangepast', event_types),
                      None)
    events = eminfra_client.search_events(created_after=datetime_first_of_month, created_by=identiteit_me, event_type=event_type)
    print(list(events))