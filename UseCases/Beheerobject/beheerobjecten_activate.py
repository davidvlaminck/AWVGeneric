import logging

from API.EMInfraClient import EMInfraClient
from API.EMInfraDomain import OperatorEnum
from API.Enums import AuthType, Environment

from UseCases.utils import load_settings

if __name__ == '__main__':
    logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(levelname)s:\t%(asctime)s:\t%(message)s',
                        filemode="w")
    eminfra_client = EMInfraClient(env=Environment.PRD, auth_type=AuthType.JWT, settings_path=load_settings())

    logging.info('Zoek inactieve beheerobjecten met child-assets.')
    logging.info('Activeer deze beheerobjecten')
    generator_beheerobjecten = eminfra_client.search_beheerobjecten(naam='DA-', actief=False,
                                                                    operator=OperatorEnum.STARTS_WITH)

    for beheerobject in iter(generator_beheerobjecten):
        if child_assets := list(eminfra_client.search_child_assets(
                asset_uuid=beheerobject.uuid, recursive=False
        )):
            logging.info(f'Activeer beheerobject ({beheerobject.naam}): {beheerobject.uuid}')
            eminfra_client.update_beheerobject_status(beheerObject=beheerobject, status=True)
        else:
            logging.info(f'Geen child-assets voor beheerobject ({beheerobject.naam}): {beheerobject.uuid}')
