from pathlib import Path

import pandas

from API.eminfra.eminfra_client import EMInfraClient
from API.Enums import AuthType, Environment


if __name__ == '__main__':
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    path_to_excel_file = Path('Camera_relaties.xlsx')
    name_relaties_tab = 'kenmerktypes'

    # pip install openpyxl
    relaties_tab = pandas.read_excel(path_to_excel_file, sheet_name=name_relaties_tab)
    relaties_tab = relaties_tab[relaties_tab['aanwezig'] == 'nee'][['assettype', 'kenmerktype']]

    assettypes = list(eminfra_client.get_all_assettypes())

    # loop over all records in relaties_tab, using columns assettype and kenmerktype
    for _, row in relaties_tab.iterrows():
        assettype_uri = row['assettype']
        kenmerktype_naam = row['kenmerktype']

        assettype = next((x for x in assettypes if x.uri == assettype_uri), None)
        if assettype is None:
            print(f'Assettype {assettype_uri} niet gevonden')
            continue

        kenmerktype = eminfra_client.get_kenmerktype_by_naam(kenmerktype_naam)
        if kenmerktype is None:
            print(f'Kenmerktype {kenmerktype_naam} niet gevonden')
            continue

        bestaande_kenmerken = eminfra_client.get_kenmerken_by_assettype_uuid(assettype.uuid)
        if any(x.kenmerkType.uuid == kenmerktype.uuid for x in bestaande_kenmerken):
            print(f'Kenmerk {kenmerktype.naam} reeds aanwezig op {assettype.uri}')
            continue

        eminfra_client.add_kenmerk_to_assettype(assettype_uuid=assettype.uuid,
                                                kenmerktype_uuid=kenmerktype.uuid)
        print(f'Kenmerk {kenmerktype.naam} toegevoegd aan {assettype.uri}')

