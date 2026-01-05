from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.TEI, auth_type=AuthType.JWT, settings_path=settings_path)

    gemigreerd_naar_kenmerk = eminfra_client.get_kenmerktype_by_naam('Gemigreerd naar')
    gemigreerd_van_kenmerk = eminfra_client.get_kenmerktype_by_naam('Gemigreerd van')

    uri_types_add_gemigreerd_naar = ['https://lgc.data.wegenenverkeer.be/ns/installatie#ZoutBijlaadPlaats',
                                    'https://lgc.data.wegenenverkeer.be/ns/installatie#Zoutsilo',]
    uri_types_add_gemigreerd_van = ['https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Zoutbijlaadplaats',
                                    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Silo',]

    assettypes = list(eminfra_client.get_all_assettypes())

    for uri_type in uri_types_add_gemigreerd_naar:
        assettype = next((x for x in assettypes if x.uri == uri_type), None)
        if assettype is None:
            print(f'Assettype {uri_type} niet gevonden')
            continue

        bestaande_kenmerken = eminfra_client.get_kenmerken_by_assettype_uuid(assettype.uuid)
        gemigreerd_naar = next((x for x in bestaande_kenmerken
                                if x.kenmerkType.uuid == gemigreerd_naar_kenmerk.uuid), None)
        if gemigreerd_naar is not None:
            print(f'Kenmerk {gemigreerd_naar_kenmerk.naam} reeds aanwezig op {assettype.uri}')
            continue

        eminfra_client.add_kenmerk_to_assettype(assettype_uuid=assettype.uuid,
                                                kenmerktype_uuid=gemigreerd_naar_kenmerk.uuid)

    for uri_type in uri_types_add_gemigreerd_van:
        assettype = next((x for x in assettypes if x.uri == uri_type), None)
        if assettype is None:
            print(f'Assettype {uri_type} niet gevonden')
            continue

        bestaande_kenmerken = eminfra_client.get_kenmerken_by_assettype_uuid(assettype.uuid)
        gemigreerd_van = next((x for x in bestaande_kenmerken
                               if x.kenmerkType.uuid == gemigreerd_van_kenmerk.uuid), None)
        if gemigreerd_van is not None:
            print(f'Kenmerk {gemigreerd_van_kenmerk.naam} reeds aanwezig op {assettype.uri}')
            continue

        eminfra_client.add_kenmerk_to_assettype(assettype_uuid=assettype.uuid,
                                                kenmerktype_uuid=gemigreerd_van_kenmerk.uuid)