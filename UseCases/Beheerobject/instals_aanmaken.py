from API.EMInfraClient import EMInfraClient
from API.Enums import AuthType, Environment
from pathlib import Path

print(""""
        Functionaliteiten gelinkt aan Beheerobject
      """)

def load_settings():
    """Load API settings from JSON"""
    settings_path = Path().home() / 'OneDrive - Nordend/projects/AWV/resources/settings_SyncOTLDataToLegacy.json'
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    return settings_path


if __name__ == '__main__':
    settings_path = load_settings()
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=settings_path)

    beheerobjecten_aan_te_maken = ['A11.121C','A25.719','N114.121C','N123.114C','N26.112','N276.212C','N34.313','N370.316C','N454.412C','N796.720','N8.312D','R26.214B','T10.415','T2.720','T74.720','T76.720','A1.121C','A11.311B','A11.311C','A11.411B','A112.121B','A112.121C','A12.121C','A14.LantisB','A17.312B','A21.121','BOL.TUNNEL','DZT.TUNNEL','LHT.TUNNEL','N1.121C','N10.214B','N10.TMB','N10.TMBB','N101.121B','N101.121C','N103.114B','N112.123B','N116.123B','N116.123C','N12.121B','N12.125B','N12.TDW','N12.TDWB','N123.125C','N123.125D','N132.125B','N141.114B','N153.123B','N156.114B','N16.112C','N171a.121B','N180.121B','N180.121C','N19.114C','N19.114D','N19g.125B','N19g.125C','N2.214B','N2.720C','N20.719B','N21.212B','N211.212B','N272.211B','N29.214C','N29.717B','N3.213B','N325.315B','N342.311B','N356a.315B','N36.316B','N368.311B','N369.313B','N369.315B','N38.313B','N38.313C','N39.313B','N39.315B','N395a.312B','N395a.312C','N395b.312B','N395b.312C','N415.412C','N436.413B','N441.412B ','N448.413B','N459.412B','N459.412C','N459.413B','N465a.412B','N494.312B','N49a.121B','N50.312B','N70.121B','N701a.719B','N715.717B','N725.717B','N773.718B','N777.720','N9.411B','OKT.TUNNEL','R0.212B','R0.212C','R1.121M','R11.121B','R14.114B','R14.114C','R2.121B','R2.121C','R2.121D','R22.212B','R22.212C','R22.TUNNEL','R23.213B','R30.311B','R42.414C','T304.312B','WEV.TUNNEL','ZAN.TUNNEL']

    for beheerobject_naam in beheerobjecten_aan_te_maken:
        generator_beheerobjecten = eminfra_client.search_beheerobjecten(naam=beheerobject_naam)
        if len(list(generator_beheerobjecten)) > 0:
            print(f'Beheerobject met naam {beheerobject_naam} bestaat al')
            continue

        response = eminfra_client.create_beheerobject(naam=beheerobject_naam)
        print(f'Beheerobject aangemaakt met als uuid: {response.get("uuid")}')
