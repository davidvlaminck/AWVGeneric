from itertools import batched

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment

def map_toestand(toestand_uri: str) -> str:
    """
    Maps the toestand URI to a human-readable state.
    :param toestand_uri: The toestand URI.
    :return: The human-readable state.
    """
    toestand_mapping = {
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik': 'IN_GEBRUIK',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/uit-gebruik': 'UIT_GEBRUIK',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/verwijderd': 'VERWIJDERD',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-ontwerp': 'IN_ONTWERP',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-opbouw': 'IN_OPBOUW',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/geannuleerd': 'GEANNULEERD',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/gepland': 'GEPLAND',
        'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/overgedragen': 'OVERGEDRAGEN'
    }
    return toestand_mapping[toestand_uri]


if __name__ == '__main__':
    from pathlib import Path

    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    # read excel niet unieke namen.xlsx
    df = pd.read_excel('niet unieke namen.xlsx', sheet_name='Blad1', header=0)
    # print all headers
    print(df.columns.tolist())

    for batch in batched(df['id'].tolist()[10000:], 100):
        filter_dict = {'uuid': batch}

        for asset in eminfra_client.asset_service.get_assets_by_filter(filter=filter_dict):
            toestand = map_toestand(asset['AIMToestand.toestand'])
            # get naam from df
            uuid = asset['@id'][39:75]
            print(uuid)
            naam = df.loc[df['id'] == uuid, 'voorstel assetnaam(legacy)'].values[0]

            if naam.startswith('CC1') and naam.endswith('/CC1'):
                naam = naam[4:-4] + '.CC1'
            elif naam.startswith('AB') and naam.endswith('/AB'):
                naam = naam[3:-3] + '.AB'
            elif naam == 'AZ0078/N133N7.63.LS':
                naam = 'N133N7.63.LS'
            elif naam.endswith('/LS'):
                naam = naam[:-3] + '.LS'
            elif naam == 'VLUCHTK. M.POS':
                naam = 'VLUCHTK.M.POS'
            commentaar = asset['AIMObject.notitie']
            isActief = asset['AIMDBStatus.isActief']
            eminfra_client.asset_service.update_asset(uuid=uuid, naam=naam, toestand=toestand, commentaar=commentaar, actief=isActief)
