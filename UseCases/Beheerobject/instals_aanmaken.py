import logging

import pandas as pd

from API.eminfra.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from UseCases.utils import load_settings

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    env = Environment.PRD
    beheerobjecten_aan_te_maken = ['XTAEV','XTAEW','XTAID','XTAIS','XTAJO','XTAOT','XTASA','XTBEL','XTBJH','XTBJS','XTBKP','XTBMK','XTEKA','XTERW','XTGBD','XTGHD','XTGZP','XTHKC','XTIOA','XTLBA','XTLGR','XTMDY','XTMNS','XTMON','XTMPS','XTMRP','XTMTA','XTMTC','XTMWY','XTNGB','XTNSS','XTOHL','XTOLL','XTOVS','XTRAW','XTRMW','XTSDD','XTTDW','XTTS107','XTTS108','XTTS111','XTTS117','XTTS119','XTTS122','XTTS123','XTTS124','XTTS125','XTTS126','XTTS128','XTTS129','XTTS137']
    logging.info('Aanmaken van beheerobjecten indien nog onbestaand.'
                 f'Omgeving: {env}'
                 f'Beheerobjecten: {beheerobjecten_aan_te_maken}')
    eminfra_client = EMInfraClient(env=env, auth_type=AuthType.JWT, settings_path=load_settings())

    rows = []
    for beheerobject_naam in beheerobjecten_aan_te_maken:
        generator_beheerobjecten = eminfra_client.beheerobject_service.search_beheerobjecten(naam=beheerobject_naam, actief=True)
        if beheerobject := next(generator_beheerobjecten, None):
            logging.info(f'Beheerobject {beheerobject.uuid} met naam {beheerobject.naam} bestaat al.')
            row = {
                "uuid": beheerobject.uuid,
                "naam": beheerobject.naam
            }
            rows.append(row)
            continue

        response = eminfra_client.beheerobject_service.create_beheerobject(naam=beheerobject_naam)
        logging.info(f'Beheerobject aangemaakt met als uuid: {response.get("uuid")}')

    output_excel_path = 'nieuwe_beheerobjecten.xlsx'
    with pd.ExcelWriter(output_excel_path, mode='w', engine='openpyxl') as writer:
        df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name='Sheet1', index=False, freeze_panes=[1, 1])